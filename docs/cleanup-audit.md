# Revisao de limpeza - 2026-09-06

Revisao realizada sobre o commit `c680ebb`, preservando a arquitetura de CLI,
orquestracao, adaptadores de coleta, workers e persistencia PostgreSQL/SQLite.
Foram examinados os arquivos versionados, documentacao, manifestos, scripts,
configuracoes Docker, imports dinamicos e testes. Nao foi encontrado AGENTS.md
no repositorio nem nos diretorios ancestrais aplicaveis.

## Alteracoes e evidencias

- `notifications.py`: removido o import de `json`. O modulo utiliza
  `urllib.parse.urlencode` para Telegram e `EmailMessage` para email; nao usa
  `json` nem o reexporta por um mecanismo de carregamento dinamico.
- `config.py`: removida a segunda tentativa UTF-8, pois `utf-8-sig` tambem
  aceita UTF-8 sem BOM. Latin-1 passou a ser o fallback final explicito:
  aceita todos os bytes, portanto a tentativa posterior com `errors=replace`
  era inalcancavel. A ordem efetiva das codificacoes foi preservada.
- `jobs.py`: simplificada a captura para `except Exception`, que ja inclui
  `ScrapeError` e sua subclasse `AuthError`. Persistencia da falha e continuidade
  da fila permanecem iguais.
- `.gitignore` e `.dockerignore`: backups `*.dump` excluidos do versionamento
  e contexto de build. O dump local preexistente foi preservado no disco.
- Testes adicionados para importacao de nomes em UTF-8 com/sem BOM, CP1252 e
  Latin-1, e para continuidade/persistencia da fila apos tres tipos de falha.

## Itens preservados

- `src/instagram_scraper.py` carrega o scraper da raiz por `importlib` e
  reexporta seus nomes. O scraper, seus simbolos expostos (inclusive `re` e
  `ceil`) e os wrappers de CLI nao foram removidos apenas por poucas referencias.
- `write_candidate_archive`, `daily_window`, aliases de comandos e metodos
  publicos do banco foram preservados como interfaces potencialmente externas.
- As migrations SQL e schemas embarcados nao foram tratados como duplicacao
  descartavel: podem atender execucao operacional e consumidores externos.
- Todas as dependencias foram mantidas. `requests` e usado pelo `gallery-dl`;
  sua declaracao direta tambem estabelece uma versao minima. `tzdata` atende
  resolucao dinamica de fusos, e `psycopg` e importado na conexao PostgreSQL.
- Fluxos de comentarios, notificacoes, downloads diretos e downloads por fila
  continuam disponiveis, mesmo quando desabilitados na configuracao local.

## Validacao e limitacoes

- Python global: teste inicial bloqueado por ausencia de `idna`. Foi criado
  um venv temporario com as dependencias declaradas, sem modificar o global.
- Windows, Python 3.11: 14 testes originais passaram antes; 16 passaram depois.
- Docker/Linux: builds original e modificado passaram. A suite final teve
  13 testes aprovados e 3 erros preexistentes em SQLite. `Database._connect`
  usa `lstrip('/')`, tornando caminhos absolutos Linux relativos. Esses mesmos
  tres testes existentes falharam na imagem original. Nao foram suprimidos.
- Os novos testes de codificacao e falhas da fila passaram tambem sobre o
  codigo original, confirmando os comportamentos que a limpeza preserva.
- Ajudas de todos os subcomandos, entry point `instagram-collector`, wrappers
  Python, `gallery-dl --version`, compilacao Python, `pip check` e
  `bash -n scripts/run_daily_cron.sh` passaram na imagem modificada sem rede.
- Ruff (`check --no-cache --select F .`): 3 apontamentos antes e 2 depois.
  Permanecem F401 para `re` e `ceil` no scraper, mantidos pela reexportacao
  dinamica. Nenhuma regra foi desabilitada.
- Nao ha configuracao de verificador de tipos ou pipeline CI versionada.
  Nao foram executadas coletas reais, notificacoes externas nem operacoes no
  banco de producao. Integracoes autenticadas e PostgreSQL nao foram testados
  de ponta a ponta nesta limpeza.

Pendencias separadas: corrigir os caminhos SQLite absolutos em Linux e definir
explicitamente a API exportada pelo scraper antes de retirar seus simbolos.
