# Operações com Ranges (`sDownload.utils.range_operations`)

Este módulo contém as funções matemáticas e algorítmicas fundamentais do **SDownload** para planejamento de conexões, monitoramento de progresso em tempo real e reconstrução de arquivos a partir de múltiplos fragmentos.

---

## 1. `calculate_ranges`

### Descrição
Calcula e particiona os intervalos de bytes (`ChunkRange`) para um download, considerando o tamanho total do arquivo, o número desejado de conexões concorrentes e eventuais pedaços já baixados ou recuperados em cache.

Quando pedaços já existem (recuperação de download interrompido):
- Preserva os pedaços já existentes.
- Identifica todos os buracos (*gaps*) entre eles.
- Divide os buracos em fatias proporcionais para download paralelo.

### Assinatura
```python
def calculate_ranges(
    file_size: int,
    num_parts: int,
    cache: list[ChunkRange] | None = None
) -> list[ChunkRange]:
```

### Parâmetros
* `file_size` (*int*): Tamanho total do arquivo em bytes.
* `num_parts` (*int*): Número alvo de conexões/divisões simultâneas.
* `cache` (*list[ChunkRange] | None*): Lista opcional de pedaços já existentes no disco/storage.

### Retorno
* `list[ChunkRange]`: Lista contínua e ordenada de intervalos cobrindo todo o arquivo de `0` até `file_size - 1`.

---

### Exemplo 1: Particionamento Inicial do Zero
```python
from sDownload.interfaces.models import ChunkRange
from sDownload.utils.range_operations import calculate_ranges

# Arquivo de 100 MB dividido em 4 partes iguais
file_size = 100 * 1024 * 1024  # 104.857.600 bytes
ranges = calculate_ranges(file_size, num_parts=4)

for r in ranges:
    print(r)
```

**Saída Esperada:**
```text
ChunkRange(start=0, end=26214399)
ChunkRange(start=26214400, end=52428799)
ChunkRange(start=52428800, end=78643199)
ChunkRange(start=78643200, end=None)
```
*(O último chunk tem `end=None` para permitir ler até o EOF sem truncamento).*

---

### Exemplo 2: Retomada com Pedaços em Cache (Preenchimento de Gaps)
```python
from sDownload.interfaces.models import ChunkRange
from sDownload.utils.range_operations import calculate_ranges

# Arquivo de 200 bytes com pedaços já baixados: [0..10] e [50..100]
cached = [ChunkRange(0, 10), ChunkRange(50, 100)]
ranges = calculate_ranges(file_size=200, num_parts=4, cache=cached)

for r in ranges:
    print(r)
```

**Saída Esperada:**
```text
ChunkRange(start=0, end=10)       # Chunk recuperado do cache
ChunkRange(start=11, end=49)      # Gap preenchido
ChunkRange(start=50, end=100)     # Chunk recuperado do cache
ChunkRange(start=101, end=150)    # Gap preenchido
ChunkRange(start=151, end=None)   # Gap final até o fim do arquivo
```

---

## 2. `calculate_downloaded_bytes`

### Descrição
Calcula o **volume total real de bytes únicos baixados** a partir das estatísticas dos chunks (`ChunkDownloadStats`). 

Projetada especificamente para o *Hot Path* (executada a cada atualização de progresso), esta função possui complexidade linear $O(k)$ e não realiza cópias de dados, garantindo telemetria leve e imune a:
* Chunks sobrepostos gerados por divisões dinâmicas (*work-stealing*).
* Bytes excedentes de socket (*buffer overshoot* de conexões canceladas).
* Chunks cancelados ou depreciados (apenas status `DOWNLOADING` e `COMPLETED` com bytes $> 0$ são contabilizados).
* Downloads em stream contínuo (`file_size=None`).

### Assinatura
```python
def calculate_downloaded_bytes(
    stats_list: Iterable[ChunkDownloadStats | None],
    file_size: int | None = None,
) -> int:
```

### Parâmetros
* `stats_list` (*Iterable[ChunkDownloadStats | None]*): Coleção de estatísticas dos chunks ativos e concluídos.
* `file_size` (*int | None*): Tamanho total do arquivo (se conhecido). Se fornecido, impede que o total ultrapasse o tamanho do arquivo.

### Retorno
* `int`: Quantidade total exata de bytes únicos baixados.

---

### Exemplo 1: Cálculo com Sobreposição de Chunks
```python
from sDownload.interfaces.models import ChunkDownloadStats, ChunkRange, EDownloadStatus
from sDownload.utils.range_operations import calculate_downloaded_bytes

# Cenário de split dinâmico onde o Chunk A baixou até 60 e o Chunk B começou no 50
chunk_a = ChunkDownloadStats(
    chunk_file_name="chunk_a.bin",
    range=ChunkRange(0, 100),
    bytes_downloaded=60,  # Cobriu [0..59] (60 bytes)
    status=EDownloadStatus.DOWNLOADING,
)

chunk_b = ChunkDownloadStats(
    chunk_file_name="chunk_b.bin",
    range=ChunkRange(50, 100),
    bytes_downloaded=30,  # Cobriu [50..79] (30 bytes)
    status=EDownloadStatus.DOWNLOADING,
)

# A união dos intervalos [0..59] e [50..79] é [0..79] = 80 bytes únicos
total_bytes = calculate_downloaded_bytes([chunk_a, chunk_b], file_size=100)
print(f"Total baixado: {total_bytes} bytes")
```

**Saída Esperada:**
```text
Total baixado: 80 bytes
```

---

### Exemplo 2: Ignorando Chunks Cancelados ou Depreciados
```python
from sDownload.interfaces.models import ChunkDownloadStats, ChunkRange, EDownloadStatus
from sDownload.utils.range_operations import calculate_downloaded_bytes

chunk_ok = ChunkDownloadStats(
    chunk_file_name="ok.bin",
    range=ChunkRange(0, 49),
    bytes_downloaded=50,
    status=EDownloadStatus.COMPLETED,
)

chunk_cancelled = ChunkDownloadStats(
    chunk_file_name="dead.bin",
    range=ChunkRange(50, 99),
    bytes_downloaded=30,  # Baixou 30 bytes antes de falhar
    status=EDownloadStatus.CANCELLED,  # Status cancelado é desconsiderado
)

total_bytes = calculate_downloaded_bytes([chunk_ok, chunk_cancelled])
print(f"Total baixado: {total_bytes} bytes")
```

**Saída Esperada:**
```text
Total baixado: 50 bytes
```

---

## 3. `calculate_optimal_coverage`

### Descrição
Resolve o problema de **reconstrução ótima do arquivo final**. 

Durante downloads multi-chunk e redimensionamentos dinâmicos (*work-stealing*), podem existir múltiplos fragmentos parciais ou redundantes no disco. Esta função modela os intervalos de chunks disponíveis como nós em um grafo direcionado e utiliza uma busca em largura (BFS / Dijkstra) para encontrar o **menor número de fragmentos contínuos** necessários para montar o arquivo de `0` até `file_size` com precisão de byte e zero sobreposição no arquivo final.

Se houver qualquer intervalo de bytes faltante (gap), a função levanta `ValueError` indicando corrupção ou arquivo incompleto.

### Assinatura
```python
def calculate_optimal_coverage(
    chunks: list[ChunkRange],
    file_size: int | None = None
) -> list[ChunkFragment]:
```

### Parâmetros
* `chunks` (*list[ChunkRange]*): Lista de todos os ranges disponíveis dos arquivos baixados.
* `file_size` (*int | None*): Tamanho total esperado do arquivo. Se `None`, busca um chunk com terminação aberta (`end=None`).

### Retorno
* `list[ChunkFragment]`: Lista ordenada de fragmentos, onde cada `ChunkFragment` contém o range do arquivo fonte e o limite de bytes a ser lido (`read_limit_qt_bytes`), garantindo que pedaços excedentes sejam cortados na fusão final.

---

### Exemplo 1: Selecionando o Menor Caminho entre Múltiplos Fragmentos
```python
from sDownload.interfaces.models import ChunkRange
from sDownload.utils.range_operations import calculate_optimal_coverage

# Fragmentos disponíveis no disco
chunks = [
    ChunkRange(0, 100),  # Chunk A: [0..100] (101 bytes)
    ChunkRange(50, 200),  # Chunk B: [50..200] (Sobreposição)
    ChunkRange(101, 300),  # Chunk C: [101..300] (Encaixe perfeito com Chunk A)
]

# Reconstruir arquivo de 301 bytes (de 0 a 300)
coverage = calculate_optimal_coverage(chunks, file_size=301)

for fragment in coverage:
    print(f"Fonte: {fragment.range} | Ler até: {fragment.read_limit_qt_bytes} bytes")
```

**Saída Esperada:**
```text
Fonte: ChunkRange(start=0, end=100) | Ler até: None bytes (lê o arquivo inteiro)
Fonte: ChunkRange(start=101, end=300) | Ler até: None bytes (lê o arquivo inteiro)
```
*(O algoritmo descartou automaticamente o Chunk B redundante e selecionou o caminho de menor número de operações de I/O).*

---

### Exemplo 2: Detectando Fragmento com Limite de Corte (Crop na Fusão)
```python
from sDownload.interfaces.models import ChunkRange
from sDownload.utils.range_operations import calculate_optimal_coverage

# Um chunk que foi estendido além do necessário
chunks = [
    ChunkRange(0, 150),  # Chunk 1: [0..150]
    ChunkRange(100, 200),  # Chunk 2: [100..200]
]

# Queremos montar até 201 bytes (0..200)
coverage = calculate_optimal_coverage(chunks, file_size=201)

for fragment in coverage:
    print(f"Fonte: {fragment.range} | Limite de leitura: {fragment.read_limit_qt_bytes}")
```

**Saída Esperada:**
```text
Fonte: ChunkRange(start=0, end=150) | Limite de leitura: 100 (lê apenas os primeiros 100 bytes)
Fonte: ChunkRange(start=100, end=200) | Limite de leitura: None (lê o restante até o fim)
```

---

## 4. Tabela Resumo

| Função | Finalidade Principal | Complexidade | Quando é Usada |
|---|---|---|---|
| `calculate_ranges` | Particionamento inicial e preenchimento de gaps no resume | $O(N \log N)$ | Início do download (`on_start`) |
| `calculate_downloaded_bytes` | Cálculo de bytes baixados para barra de progresso e estatísticas | $O(k \log k)$ | Em cada tick do download (`on_update`) |
| `calculate_optimal_coverage` | Resolução do grafo de montagem e corte de arquivos | $O(V + E)$ (BFS) | Finalização e fusão do arquivo (`_finalize`) |
