# DownloadTask: Breakdown de Implementação (Próximos Passos)

## Contexto Geral

O `DownloadTask` é o orquestrador de alto nível. Ele delega todas as decisões estratégicas (quantos chunks abrir, quando dividir, quando pausar um mirror) para um `DownloadStrategyProtocol` injetado. O trabalho dele é puramente **mecânico**: fazer o HEAD da URL (via `get_file_info`), gerenciar o ciclo de vida do `ChunkManager`, executar as ordens cegas da estratégia e lidar com pausas/resumes/cancelamentos visíveis para o usuário.

---

## 🛠️ Tarefa 0 — Arquitetura de Extratores (Strategy) e Simplificação do Crawler

A descoberta de recursos via HTTP (HTML scraping, JSON parsing, WebDAV) não deve poluir o `HttpxDownloader`. Além disso, a função `list_resources` atual é muito pesada por realizar requisições sequenciais (`get_file_info`) para descobrir os metadados de tudo o que encontra pela frente.

**O que faremos nesta tarefa (Refatoração de Performance e Padrão Strategy):**

1. **Alterar as Assinaturas no `DownloaderProtocol`:**
   Mudar o retorno de `list_resources` para gerar apenas `strings` (as URLs) em vez de objetos pesados `ResourceInfo`. Quem precisar dos metadados chama o `get_file_info` posteriormente (e em paralelo).
   ```python
   async def list_resources(
       self, 
       url: str, 
       pattern: str | None = None,
       level: int = 1
   ) -> AsyncGenerator[str, None]:
   ```

2. **Criar o Padrão Extractor Strategy (`sDownload/http_client/extractors/`):**
   A arquitetura seguirá esta estrutura de pastas para isolar totalmente a lógica do downloader:
   ```text
   sDownload/http_client/
   ├── httpx_downloader.py
   ├── httpx_error_mapper.py
   └── extractors/                  <-- NOVO PACOTE
       ├── __init__.py
       ├── protocol.py              <-- Interface base (ResourceExtractorProtocol)
       ├── factory.py               <-- Descobre qual extrator usar (Faz o HEAD)
       ├── html_extractor.py        <-- Parseia <a href="...">
       ├── json_extractor.py        <-- Parseia strings com "http..."
       └── webdav_extractor.py      <-- Cria e processa requests XML (PROPFIND)
   ```
   **Arquivos Chave:**
   - **`protocol.py`**: Define a interface com a assinatura `async def extract(self, url: str, client: httpx.AsyncClient) -> AsyncGenerator[str, None]:`.
   - **`factory.py` (A Sonda):** A classe `ExtractorFactory` que faz um `HEAD` rápido para descobrir o `Content-Type` ou Headers DAV e decide a estratégia correspondente.

3. **Orquestração Inteligente (BFS) no `HttpxDownloader`:**
   Limpar completamente a classe `HttpxDownloader` de lógica de Parsing/Scraping. O `list_resources` será exclusivamente um gerenciador de fila (BFS - Busca em Largura):
   - Avalia o nível de profundidade atual.
   - Pede à `ExtractorFactory` a melhor estratégia.
   - Usa o gerador do Extractor e passa pelo filtro `regex` (se `pattern` fornecido).
   - Adiciona links no final da fila se restarem níveis recursivos.

4. **Ambiente de Testes Avançado (WebDAV nativo):**
   - **HTML/JSON:** Utilizar o ambiente estático e seguro do Nginx (`scenarios_pages_html/teste1`) que já construímos e funciona perfeitamente para arquivos da web estáticos.
   - **Nginx WebDAV Constraint:** Como o Nginx oficial não suporta nativamente o método HTTP `PROPFIND` em WebDAV, vamos abandoná-lo para testes WebDAV de exploração profunda.
   - **Novo Container Testcontainers:** 
     - Modificar o `conftest.py` criando uma nova *fixture* assíncrona que sobe dinamicamente a imagem de docker leve: `bytemark/webdav`.
     - Este será um servidor em uma porta distinta com diretórios criados no build up da *fixture*.
     - **Testes PROPFIND**: Executar requisições autênticas contra ele para validar a comunicação e o roteiro do `webdav_extractor.py`, garantindo alta manutenibilidade do serviço de crawler.


---

## 📋 Tarefa 1 — `_resolve_file_info()` → O Sanity Check Inicial

Com o protocolo atualizado (Tarefa 0), esta etapa fica muito mais limpa e focada em preparar o estado para o ChunkManager.

### Ideia de Código:
```python
async def _resolve_file_info(self) -> None:
    """
    Busca metadados do servidor, define o nome final do arquivo e valida o suporte a chunks.
    Salva o resultado em self._resource_info e ajusta estado interno se necessário.
    """
    # 1. Busca os metadados
    self._resource_info = await self._downloader.get_file_info(self._params.url)
        
    # 2. Define o nome do arquivo final
    self._file_name = (
        self._params.file_name or 
        self._resource_info.file_name or 
        "downloaded_file.bin"
    )
    
    # 3. Validação de segurança sobre conexões paralelas
    self._use_chunked = self._params.use_chunked
    if self._use_chunked and not self._resource_info.server_accept_ranges:
        logger.warning(
            f"O servidor não suporta download em partes. "
            f"Forçando modo single-chunk."
        )
        self._use_chunked = False
```

**O que faz (Passo a passo):**
1. **Bate na fonte:** Ao iniciar o download, chama `await downloader.get_file_info(self._params.url)`. Como combinado, isso retorna a ficha limpa e direta de UM único arquivo (`ResourceInfo`).
2. **Resolve o Nome Final:** Qual nome vamos usar para salvar no HD? O `ResourceInfo` já traz uma sugestão de nome (tirada do header ou URL).
   - Se o usuário passou explicitamente no construtor (`self._params.file_name`), nós **sobreescrevemos/preferimos** o do usuário.
   - Se não passou, **usamos o nome que veio do servidor** (`info.file_name`).
3. **Limpa o modo Chunked:** 
   - Se o usuário configurou `params.use_chunked = True`, mas o servidor devolveu `info.server_accept_ranges = False`, a aplicação deve ignorar a vontade do usuário e forçar o `use_chunked = False` internamente. Tentar criar multiconexões num servidor ignorante só gera arquivos corrompidos e lixo na rede.

**Inputs Internos:** `self._params.url`, `self._params.file_name`, `self._params.use_chunked`
**Outputs Internos:** Preenche `self._resource_info` e `self._file_name`. Ajusta `self._params.use_chunked` se necessário.

---

## 📋 Tarefa 2 — `_init_chunk_manager()` → Preparar o Terreno

**O que faz:**
- Cria a instância vazia de `DownloadStats(file_size=resource_info.file_size)`.
- **Modo Recovery:** Se `params.enable_recovery == True`:
  - Chama a classe `RecoveryDownload` para ver se já existe um `.sdownload` ou arquivo de metadados salvo.
  - Se existir, carrega os `recovered_stats` e já injeta no `DownloadStats.bytes_downloaded`.
- Constrói o `ChunkManagerParams` juntando informações do `resource_info` e do `params` do usuário.
- Inicializa o `ChunkManager` injetando o downloader, storage, e (se houver) os `recovered_stats`.

**Decisão Importante:**
- Download novo = O `ChunkManager` nasce "limpo".
- Resume = O `ChunkManager` já nasce sabendo onde parou e com os arquivos na pasta.

---

## 📋 Tarefa 3 — `start()` → O Método Público Principal

Esta é a cola que une as Tarefas 1 e 2 e dá o pontapé inicial:

**Ordem de execução:**
1. **Guarda de Estado:** Se `self._status != PENDING`, aborta lançando `LifecycleError` (ex: tentar dar start em algo pausado — deveria ser `resume()`).
2. Chama a **Tarefa 1** (`_resolve_file_info()`).
3. Chama a **Tarefa 2** (`_init_chunk_manager()`).
4. Consulta a estratégia pela primeira vez: chama `strategy.on_start(dl_stats, chunk_manager.stats, max_conn)`.
5. Recebe as ações iniciais (ex: 4 instâncias de `StartChunkAction`) e executa cada uma no `ChunkManager.start_chunk()`.
6. Muda o estado interno para `self._status = DOWNLOADING`.
7. **Motor Auxiliar:** Cria a task de background infinita chamando `asyncio.create_task(self._dl_controller())`.

---

## 📋 Tarefa 4 — `_dl_controller()` → O Cérebro (Loop Assíncrono)

Roda em background e só termina quando o download acaba ou é cancelado/pausado.

**Como funciona o loop:**
1. Fica bloqueado esperando chunks terminarem: `completed_chunks = await chunk_manager.wait_for_completed_chunks()`.
2. Quando acorda, atualiza o progresso global no `DownloadStats`.
3. Chama `strategy.on_update(...)` para saber os próximos passos.
4. Executa as ações devolvidas pela estratégia (criar mais chunks, redimensionar, etc).

**Política de Resposta a Erros:**
Para cada chunk que voltou com erro (`stats.last_error` preenchido):
- Se for **Erro de Rede/Timeout** (`CommunicationError`) → Tenta de novo no mesmo range, ou pede pra estratégia escolher um mirror de fallback.
- Se for **Erro de Corrupção** (`IntegrityError`) → Tenta baixar aquele range novamente.
- Se for **Disco Cheio / Sem Permissão** (`StorageError`) → Falha Crítica. Cancela todos os outros chunks, aborta o loop, muda o status do pai para `ERROR`.

---

## 📋 Tarefa 5 — Ciclo de Vida (`pause()`, `resume()`, `cancel()`)

### `pause()`
- Cancela o loop do `_dl_controller`.
- Pede para o `ChunkManager` cancelar todos os chunks ativos (`cancel_all_chunks()`).
- Se `enable_recovery == True`: Manda salvar o estado atual (`chunk_manager.stats`) no disco.
- Estado vira `PAUSED`.

### `resume()`
- Proteção: Se status não é `PAUSED`, lança erro.
- Executa os passos de Recovery (semelhante a Tarefa 2) para recriar o `ChunkManager`.
- Pula as requisições web (HEAD) e pula direto para o passo 4 do `start()` (aciona a estratégia e liga o loop).

### `cancel(delete_temp_files=True)`
- Cancela o loop.
- Cancela os chunks ativos.
- Se `delete_temp_files`: avisa o `ChunkManager` para expurgar do HD.
- Apaga os metadados de recovery se existirem.
- Estado vira `CANCELLED`.

---

## 📋 Tarefa 6 — `_finalize()` → O Grito de Vitória

Executado quando o loop da Tarefa 4 detecta que o tamanho baixado bate com o `file_size`.

1. Chama `await chunk_manager.merge_chunks()` para costurar os `.bin`.
2. Move e renomeia o arquivo final para o `dest_dir` do usuário.
3. Apaga os arquivos de controle (JSON de recovery).
4. Seta o `DownloadStats` cravado em 100% de progress.
5. Muda o status final para `COMPLETED`.
6. Emite o sinal para quem estava aguardando via `wait_until_done()`.

---

## ❓ Questões Abertas para Discussão (PM e Tech Lead)

| # | Pergunta/Decisão | Sugestão Padrão |
|---|------------------|-----------------|
| 1 | Quantos retries vamos tentar individualmente em um chunk (por exemplo, erro de conexão) antes de decidir que o download inteiro falhou? | 3 tentativas (podemos parametrizar). |
| 2 | Tratamento de Mirrors: O protocolo `get_file_info()` permite retornar uma *lista* de `ResourceInfo`. Onde guardamos a lógica de tentar o mirror B se o mirror A falhar? No Task ou na Strategy? | No `DownloadTask` (o task esconde isso da strategy, ou atualiza a strategy com a URL nova). |
| 3 | A tipagem de `DownloadStats.file_size` está como `int`. Mas e se o servidor não informar o tamanho (ex: sem header Content-Length)? Recomendo mudar pra `Optional[int]`. | Mudar para `Optional[int]` e corrigir o cálculo de %. |
