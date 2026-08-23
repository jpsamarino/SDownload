# AGENTS.md - SDownload Project Context

## Visão Geral

- **Projeto**: SDownload - Download manager assíncrono com suporte a multi-chunk, WebDAV, HTTP(S)
- **Linguagem**: Python 3.12+
- **Virtual Environment**: `.venv` na raiz do projeto

## Como Rodar Testes

### Comando base

```bash
.\venv\Scripts\pytest -v <caminho_do_arquivo_ou_pasta>
```

### Exemplos

```bash
# Teste específico
.\venv\Scripts\pytest -v tests/http_client/test_httpx_downloader.py

# Pasta de testes
.\venv\Scripts\pytest -v tests/http_client/

# Todos os testes
.\venv\Scripts\pytest -v tests/
```

## Como Rodar Linting e Formatação

### Ruff (Linter & Formatter Oficial)

```bash
# Verificar lint
.\venv\Scripts\ruff.exe check .

# Aplicar correções automáticas
.\venv\Scripts\ruff.exe check --fix .

# Formatar código
.\venv\Scripts\ruff.exe format .

# Checar formatação
.\venv\Scripts\ruff.exe format --check .
```

### Configurações do Ruff (`ruff.toml`)

- Target Python: `py312`
- Max line length: 100
- Regras ativas: `E`, `W`, `F`, `I` (isort), `UP` (pyupgrade), `B` (bugbear), `C4`, `SIM`

## Estrutura do Projeto

```
sDownload/
├── exceptions/          # Exceções customizadas
├── file_system/        # Armazenamento local
├── http_client/        # Cliente HTTP (httpx) e discovery
│   └── extractors/     # Strategy pattern para HTML, JSON, WebDAV
├── interfaces/        # Protocolos e modelos de dados
│   └── models/        # Dataclasses e enums
├── services/          # Serviços de download
│   └── downloader_manager/
│       ├── chunk_utils/
│       ├── strategies/
│       └── throttling/
├── telemetry/          # Logging
└── utils/             # Utilitários (URL, range, etc)
```

## Arquitetura

### Módulos Principais

- `sDownload/http_client/` - Cliente HTTP (httpx) e discovery de recursos
- `sDownload/services/` - Serviços de download (chunk manager, recovery)
- `sDownload/interfaces/` - Protocolos e modelos de dados
- `sDownload/utils/` - Utilitários (URL, range operations, etc)
- `sDownload/file_system/` - Armazenamento local
- `sDownload/exceptions/` - Exceções customizadas
- `sDownload/telemetry/` - Logging

### Padrões de Design

- **Strategy Pattern**: Extractors para diferentes content-types (HTML, JSON, WebDAV)
- **Protocol**: Interfaces definem contratos (DownloaderProtocol, FileStorageProtocol)
- **AsyncIO**: Todo o http_client é assíncrono

### Dataclasses e Enums Importantes

- `DiscoveryResult`: Resultado de exploração de recursos (files, directories, unresolved_links)
- `DiscoveryTask`: Task para fila de crawling (url, level, method_hint, process_only_files)
- `ExtractedLink`: Link extraído com classificação (is_dir: bool|None)
- `DiscoveryMethod`: Enum (GET, PROPFIND, POST, UNKNOWN)

### Hierarquia de Exceptions

```
sDownload.exceptions.BaseSDownloadError
├── CommunicationError
├── DataError
└── InfrastructureError
```

## Convenções de Código

### Python

- Type hints obrigatórios em funções públicas
- Docstrings em classes e funções públicas
- Max line length: 100
- Imports ordenados: stdlib → third-party → local

### Nomenclatura

- Classes: `PascalCase`
- Funções/Variáveis: `snake_case`
- Constantes: `SCREAMING_SNAKE_CASE`
- Types/Enums: `PascalCase`

### Git

- Branch: `feature/`, `fix/`, `refactor/`
- Commits: verbum no imperativo ("Add feature", "Fix bug")

## Notas Técnicas

### Resource Discovery (list_resources)

- Usa BFS com fila de `DiscoveryTask`
- `level` controla profundidade máxima de exploração
- `unknown_links` são links que precisam de probe OPTIONS para confirmar se são arquivos
- `sub_nodes` são diretórios/páginas navegáveis
- `only_files=True` faz probe rápido para confirmar se URL é arquivo

### HTTP Client

- Usa `httpx.AsyncClient` com streaming
- Suporta range requests para resume de downloads
- Timeout configurável via `HttpConfigModel`
- Suporte a proxy, cookies, headers customizados
