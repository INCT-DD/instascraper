# Instascraper

Pipeline de coleta acadêmica para monitoramento longitudinal de perfis públicos do Instagram. O projeto coleta publicações e stories, preserva metadados e mídias, registra a execução no PostgreSQL e organiza artefatos para análise e compartilhamento.

O sistema foi desenhado para execução local ou em servidor, por linha de comando e Docker Compose. Não há interface web.

## Capacidades

- coleta diária ou retroativa de posts por perfil e período;
- coleta dos stories disponíveis no momento da execução;
- modo incremental que encerra a paginação ao alcançar posts já armazenados;
- atualização posterior de curtidas, comentários, visualizações e reposts;
- download assíncrono de mídias de posts e stories;
- rotação controlada de contas autenticadas;
- persistência em PostgreSQL, com SQLite disponível para testes;
- exportação por candidato e data de referência;
- retomada de perfis incompletos e repetição apenas de falhas;
- logs, relatórios e notificações de execução agendada.

## Documentação

| Documento | Conteúdo |
|---|---|
| [Referência da CLI](docs/cli.md) | Todos os comandos, opções, combinações e códigos de saída. |
| [Arquitetura e operação](docs/pipeline.md) | Componentes, fluxo de dados, banco, filas, Docker, servidor e agendamento. |
| [Coleta de posts e métricas](docs/gallery-dl-posts.md) | Backends, paginação, modo incremental, campos e atualização de métricas. |
| [Auditoria de limpeza](docs/cleanup-audit.md) | Registro histórico das simplificações e validações de manutenção. |

## Início rápido

### 1. Preparar os arquivos locais

```powershell
Copy-Item .env.example .env
Copy-Item profile.example.json profiles.json
Copy-Item sessions.example.json sessions.json
```

Edite `.env`, `profiles.json` e `sessions.json`. Esses arquivos e o diretório `cookies/` contêm configuração local ou credenciais e não devem ser versionados.

Cada sessão pode utilizar dois formatos de cookie:

- JSON para requisições feitas pelos coletores Python;
- Netscape `cookies.txt` para o `gallery-dl`.

### 2. Subir os serviços

```powershell
docker compose up -d --build
```

O Compose mantém três serviços:

- `postgres`: banco de dados persistente;
- `app`: ambiente para comandos da pipeline;
- `media-worker`: consumidor contínuo da fila de mídias.

### 3. Preparar o banco e os perfis

```powershell
docker compose run --rm app python -m pipeline migrate
docker compose run --rm app python -m pipeline seed-profiles
```

As migrations só precisam ser executadas na primeira instalação e depois de uma atualização que altere o schema.

### 4. Executar uma coleta diária

```powershell
docker compose run --rm app python -m pipeline run-daily --date 2026-09-15 --new-only --skip-jobs --rps 0.2
```

Esse comando coleta posts e stories. `--new-only` afeta somente posts; stories são sempre consultados entre os itens atualmente disponíveis. `--skip-jobs` desativa o processamento de comentários naquela execução e não interfere no `media-worker`.

## Fluxos usuais

### Posts de um período

```powershell
docker compose run --rm app python -m pipeline collect-posts --start-date 2026-09-01 --end-date 2026-09-15 --no-comments --new-only --rps 0.2
```

As datas são inclusivas. A coleta incremental ainda consulta a primeira página de cada perfil, mas não envia posts conhecidos ao `upsert` e evita continuar a paginação depois do primeiro post regular conhecido.

### Stories de hoje

```powershell
docker compose run --rm app python -m pipeline collect-stories --date 2026-09-15
```

Quando a fila de mídia está habilitada, o comando cria jobs de stories com prioridade superior aos jobs de posts. O worker passa a atendê-los antes porque stories expiram rapidamente.

### Atualizar métricas de posts existentes

```powershell
docker compose run --rm app python -m pipeline refresh-post-metrics --start-date 2026-09-01 --end-date 2026-09-15 --limit 100 --rps 0.1
```

A atualização consulta o detalhe de cada post selecionado. Campos omitidos pelo Instagram não apagam valores anteriores; zero é preservado como medição válida. O payload de auditoria é registrado em `raw_payloads` com o tipo `post_metrics`.

### Retomar uma coleta interrompida

```powershell
docker compose run --rm app python -m pipeline collect-posts --start-date 2026-09-01 --end-date 2026-09-15 --no-comments --resume --rps 0.2
```

`--resume` seleciona perfis incompletos para o intervalo exato. `--retry-failed` seleciona apenas perfis cuja tentativa mais recente falhou. As opções são mutuamente exclusivas e não retomam um cursor interno de página: cada perfil selecionado recomeça pela primeira página.

### Acompanhar mídias

```powershell
docker compose logs -f media-worker
```

Posts persistidos e mídias concluídas são estados diferentes. Um post pode estar no banco enquanto o job de mídia permanece `pending`, `retry`, `processing` ou `failed`.

## Organização dos dados

O PostgreSQL é a fonte de verdade para entidades e estado operacional. Os principais conjuntos são:

- `profiles`: perfis monitorados;
- `posts`: publicações e métricas normalizadas;
- `post_media`: itens de mídia e estado do download;
- `stories`: metadados de stories preservados;
- `collection_jobs`: filas de comentários, mídias e stories;
- `collection_runs` e `profile_collection_status`: histórico de execução;
- `raw_payloads`: respostas brutas para auditoria.

Arquivos persistentes são separados por finalidade:

```text
data/       dados brutos e auxiliares
exports/    JSONs e mídias organizados para consulta externa
logs/       logs da aplicação
reports/    relatórios de execução
```

`HOST_EXPORTS_DIR` pode apontar para uma pasta sincronizada pelo OneDrive no computador hospedeiro. O container continua escrevendo em `/app/exports`; o cliente do OneDrive é responsável pela sincronização com a nuvem.

## Configuração mínima

As principais variáveis do `.env` são:

| Variável | Finalidade |
|---|---|
| `POSTGRES_*` | Conexão e credenciais do PostgreSQL. |
| `PROFILES_PATH` | JSON, CSV ou diretório com perfis monitorados. |
| `SESSIONS_PATH` | Lista de sessões e arquivos de cookies. |
| `RPS` | Requisições por segundo para coleta de posts. |
| `POSTS_BACKEND` | `auto`, `graphql`, `scraper` ou `gallery-dl`. |
| `ACCOUNT_ROTATION_ENABLED` | Habilita alternativas de sessão configuradas. |
| `MEDIA_QUEUE_ENABLED` | Enfileira downloads em vez de executá-los no processo coletor. |
| `COLLECT_POST_MEDIA` | Habilita preservação das mídias dos posts. |
| `GALLERY_DL_SLEEP_REQUEST` | Intervalo entre requisições do `gallery-dl`. |
| `GALLERY_DL_SLEEP_DOWNLOAD` | Intervalo entre downloads do `gallery-dl`. |
| `HOST_EXPORTS_DIR` | Pasta do host montada em `/app/exports`. |

Consulte `.env.example` para a lista completa e os valores padrão do Compose.

## Operação responsável

- Use somente contas autorizadas para a pesquisa.
- Mantenha cookies, senhas, tokens e dumps fora do Git.
- Adote uma taxa conservadora e acompanhe respostas `401`, `403`, `429`, `challenge_required` e `feedback_required`.
- Rotação de contas melhora continuidade operacional, mas não elimina limites da plataforma.
- Stories devem ser coletados enquanto ainda estão disponíveis; uma execução retroativa não recupera conteúdo expirado.
- URLs de CDN são temporárias. O download deve ocorrer próximo da coleta ou ser precedido pela renovação das URLs falhadas.
- Defina política de retenção, controle de acesso e base ética para dados pessoais e conteúdo público coletado.

## Verificação do ambiente

```powershell
docker compose ps
docker compose run --rm app python -m pipeline --help
docker compose exec postgres pg_isready -U collector -d instagram_collector
```

Para detalhes de instalação sem Docker, cron, backups, notificações e diagnóstico de falhas, consulte [Arquitetura e operação](docs/pipeline.md).
