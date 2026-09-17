# Arquitetura e operação da pipeline

Este documento descreve os componentes, os limites de responsabilidade e os procedimentos operacionais do Instascraper. A lista exata de comandos e opções está em [Referência da CLI](cli.md).

## Visão arquitetural

A aplicação segue uma arquitetura modular orientada a casos de uso. A CLI recebe o comando, a camada de pipeline coordena coletores e persistência, e os adaptadores isolam Instagram, `gallery-dl`, banco e sistema de arquivos.

```text
CLI
 ├─ coleta diária ou por período
 ├─ manutenção de métricas e mídias
 ├─ processamento de filas
 └─ exportação e administração
        │
        v
Pipeline de aplicação
 ├─ seleção de perfis e sessões
 ├─ controle de taxa e retomada
 ├─ normalização e persistência
 └─ criação de jobs
        │
        ├─> PostgreSQL
        ├─> Instagram GraphQL/REST
        ├─> gallery-dl
        └─> exports, logs e relatórios
```

## Componentes

| Componente | Responsabilidade |
|---|---|
| `pipeline.py` | Ponto de entrada para `python -m pipeline`. |
| `src/instagram_collector/cli.py` | Parser, validação de argumentos e despacho dos casos de uso. |
| `src/instagram_collector/pipeline.py` | Orquestra coleta diária, períodos, perfis, sessões e exports. |
| `src/instagram_collector/graphql_posts.py` | Timeline GraphQL e paginação de posts. |
| `src/instagram_collector/scraper.py` | Adaptador REST para posts e comentários. |
| `src/instagram_collector/gallerydl_posts.py` | Fallback de posts via API REST utilizada pelo `gallery-dl`. |
| `src/instagram_collector/gallerydl.py` | Coleta e download de stories. |
| `src/instagram_collector/post_metrics.py` | Atualização das métricas de posts já persistidos. |
| `src/instagram_collector/media_jobs.py` | Worker e regras da fila de mídia. |
| `src/instagram_collector/jobs.py` | Fila opcional de comentários e respostas. |
| `src/instagram_collector/storage.py` | Acesso PostgreSQL/SQLite, schema, upserts e consultas operacionais. |
| `src/instagram_collector/files.py` | Arquivos brutos, relatórios e estrutura de exportação. |
| `src/instagram_collector/sessions.py` | Sessões, aliases e rotação de contas. |
| `src/instagram_collector/notifications.py` | Telegram e SMTP para execuções agendadas. |

## Serviços Docker

O `docker-compose.yml` define três serviços:

### postgres

PostgreSQL 16 com health check e volume `postgres_data`. A porta pode ser exposta ao host por `POSTGRES_HOST_PORT`.

### app

Imagem da aplicação mantida ativa para comandos pontuais. O uso normal é:

```powershell
docker compose run --rm app python -m pipeline <comando>
```

### media-worker

Executa continuamente `process-media-queue --watch`. O serviço compartilha banco, cookies e diretórios com o `app`. Alterações de configuração exigem recriação do container:

```powershell
docker compose up -d --build --force-recreate media-worker
```

## Persistência e volumes

| Volume ou montagem | Conteúdo |
|---|---|
| `postgres_data` | Banco PostgreSQL. |
| `media_data` | Dados locais sob `/app/data`. |
| `logs_data` | Logs sob `/app/logs`. |
| `reports_data` | Relatórios sob `/app/reports`. |
| `HOST_EXPORTS_DIR` | Pasta do host montada em `/app/exports`. |
| `profiles.json` | Perfis, somente leitura. |
| `sessions.json` | Sessões, somente leitura. |
| `cookies/` | Cookies, somente leitura. |

O banco permanece no computador ou servidor que executa o Docker. Montar `HOST_EXPORTS_DIR` em uma pasta do OneDrive transfere exports e mídias para essa pasta, mas não move o PostgreSQL para a nuvem.

## Modelo de dados

| Tabela | Papel |
|---|---|
| `profiles` | Cadastro e regras de cada perfil. |
| `posts` | Publicações normalizadas e métricas atuais. |
| `post_media` | Mídias identificadas, URL de origem, caminho e status. |
| `stories` | Stories preservados durante a janela de disponibilidade. |
| `comments` e `replies` | Dados opcionais de interação textual. |
| `collection_jobs` | Jobs de comentários, mídias de posts e stories. |
| `collection_runs` | Execuções globais e seus resultados. |
| `profile_collection_status` | Resultado de cada perfil por tipo e intervalo. |
| `raw_payloads` | Evidência bruta de coleta e atualização. |

Os identificadores da plataforma e shortcodes possuem restrições de unicidade. Repetir uma coleta é seguro para os registros principais: o `upsert` insere dados novos e atualiza o que já existe.

## Fluxo de posts

1. A CLI valida período e opções.
2. Os perfis são sincronizados de `profiles.json` ou CSV para o banco.
3. A pipeline seleciona perfis ativos, ou um único `--username`.
4. O pool escolhe uma sessão autenticada.
5. O backend lista páginas até o limite temporal ou incremental.
6. Cada post é normalizado e persistido.
7. O payload bruto é registrado para auditoria.
8. As mídias identificadas são persistidas em `post_media`.
9. Jobs de mídia são criados quando a fila está habilitada.
10. O resultado do perfil e da execução é gravado separadamente.

Uma falha de perfil é registrada e a coleta segue para os demais. Códigos de saída diferentes de zero informam que houve falha ou resultado parcial.

## Fluxo de stories

Stories não possuem coleta histórica equivalente à timeline de posts. O comando consulta o conjunto disponível naquele momento e cria um job por perfil. O job recebe prioridade superior à mídia de posts e é processado primeiro pelo worker.

Reexecutar a coleta não duplica jobs equivalentes. Entretanto, stories que expiraram antes da consulta não podem ser recuperados pela pipeline.

## Filas

### Mídias

O `media-worker` busca jobs pendentes por prioridade. Stories têm prioridade operacional sobre posts. `MEDIA_QUEUE_LIMIT` limita o lote de cada ciclo, não o total histórico da fila. `MEDIA_WORKER_SLEEP_SECONDS` controla a espera quando o modo `--watch` volta a consultar o banco.

Estados usuais: `pending`, `processing`, `retry`, `done` e `failed`.

### Comentários

Comentários e respostas são opcionais e ficam desabilitados quando `COLLECT_COMMENTS_DEFAULT=false`. `--skip-jobs` evita processar essa fila durante a execução diária; não controla o worker de mídia.

## Perfis e sessões

### Perfis

`PROFILES_PATH` aceita:

- um JSON com uma lista de perfis;
- um CSV;
- um diretório com múltiplos JSONs e CSVs.

Exemplo:

```json
[
  {
    "name": "Perfil de exemplo",
    "username": "perfil.exemplo",
    "active": true,
    "priority": 10,
    "notes": "grupo de pesquisa"
  }
]
```

`profiles.json` é local. `profile.example.json` é o modelo versionado.

### Sessões

```json
[
  {
    "name": "collector-01",
    "active": true,
    "instagram_cookie_json": "cookies/collector-01.json",
    "gallery_dl_cookies": "cookies/collector-01.txt"
  }
]
```

Com `ACCOUNT_ROTATION_ENABLED=true`, uma operação compatível pode tentar sessões alternativas após falha. A rotação não ocorre a cada requisição e não deve ser usada para contornar restrições da plataforma.

## Instalação sem Docker

Em Debian/Ubuntu:

```bash
sudo apt install python3 python3-venv postgresql
python3 -m venv venv
source venv/bin/activate
python -m pip install -e .
python -m pipeline migrate
python -m pipeline seed-profiles
```

No modo nativo, `POSTGRES_HOST` deve apontar para `localhost`, não para o hostname Docker `postgres`.

## Agendamento no servidor

O agendador usa uma pipeline sequencial: interrompe o `media-worker`, conclui a raspagem de posts e stories e inicia o worker somente depois. Ele inicia o worker mesmo quando alguns perfis falham, permitindo baixar as mídias dos perfis bem-sucedidos, e preserva o código de saída da coleta para o cron.

No Windows, execute:

```powershell
.\scripts\run_daily_pipeline.ps1 --date 2026-09-15 --new-only --rps 0.2
```

No Linux, use `scripts/run_daily_cron.sh`. Uma falha ao iniciar o worker tem precedência sobre o código da coleta.

O script `scripts/run_daily_cron.sh` executa `run-scheduled --skip-jobs --export`. Exemplo de crontab:

```cron
15 8 * * * cd /srv/instascraper && ./scripts/run_daily_cron.sh >> logs/cron-daily.log 2>&1
```

Antes de cadastrar:

```bash
chmod +x scripts/run_daily_cron.sh
./scripts/run_daily_cron.sh --date 2026-09-15
```

O cron executa comandos; ele não mantém sessão de terminal aberta. Caminhos devem ser absolutos e o usuário do cron precisa ter acesso ao Docker, aos cookies e à pasta de exports.

## Notificações

`run-scheduled` pode notificar falhas por Telegram ou SMTP. Variáveis principais:

```env
NOTIFY_ENABLED=true
NOTIFY_PROVIDER=telegram
NOTIFY_ON_SUCCESS=false
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

Para SMTP, configure `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM` e `SMTP_TO`. Segredos devem permanecer apenas no `.env` do servidor.

## Backup e restauração

Backup lógico:

```powershell
docker compose exec -T postgres pg_dump -U collector -d instagram_collector -Fc > instagram_collector.dump
```

Restauração em banco vazio:

```powershell
docker compose cp .\instagram_collector.dump postgres:/tmp/instagram_collector.dump
docker compose exec postgres pg_restore -U collector -d instagram_collector --clean --if-exists /tmp/instagram_collector.dump
```

O dump não inclui `exports/`, mídias, cookies nem logs. Esses diretórios exigem backup separado.

## Diagnóstico

### Banco indisponível

```powershell
docker compose ps
docker compose logs postgres
docker compose exec postgres pg_isready -U collector -d instagram_collector
```

### Worker sem processar

```powershell
docker compose logs --tail 100 media-worker
docker compose up -d --force-recreate media-worker
```

### Respostas 401, 403 ou redirect

Verifique validade dos cookies, correspondência entre JSON e Netscape, caminho montado no container e estado da conta. O navegador não precisa permanecer aberto depois que os cookies foram exportados, mas logout, troca de senha e desafios podem invalidá-los.

### Resposta 429

Reduza `RPS`, aumente os intervalos do `gallery-dl`, suspenda processos concorrentes e aguarde antes de repetir. O coletor de posts e o worker fazem tráfego independente.

### URLs de mídia expiradas

Use `refresh-failed-media` para renovar apenas posts com mídia falhada ou em retry no intervalo desejado e depois deixe o worker processar a fila novamente.

## Atualização do projeto

```bash
git pull
docker compose build app media-worker
docker compose run --rm app python -m pipeline migrate
docker compose up -d --force-recreate app media-worker
```

Execute `migrate` após atualizar o código; o comando é idempotente. Não substitua `.env`, `profiles.json`, `sessions.json` ou `cookies/` pelos arquivos de exemplo.
