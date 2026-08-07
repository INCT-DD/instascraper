# Pipeline diaria de coleta academica

Esta estrutura preserva `instagram_scraper.py` e adiciona um pipeline leve em
`src/instagram_collector/`. O scraper atual continua sendo o motor de posts,
comentarios e replies. O `gallery-dl` entra como coletor complementar de stories
disponiveis no momento da execucao.

## Arquitetura

- `instagram_scraper.py`: fluxo legado interativo e funcoes reaproveitadas.
- `profiles.json`: lista configuravel de perfis monitorados.
- `sessions.json`: lista local de contas/cookies. Nao versionar este arquivo.
- `migrations/001_init.sql`: schema PostgreSQL versionado.
- `src/instagram_collector/scraper.py`: wrapper nao interativo do motor atual.
- `src/instagram_collector/gallerydl.py`: runner isolado do `gallery-dl` para stories.
- `src/instagram_collector/jobs.py`: fila de comentarios/replies com retry e limites.
- `src/instagram_collector/pipeline.py`: orquestracao diaria/manual.
- `src/instagram_collector/files.py`: NDJSON, CSV, relatorios e export.

## Docker Compose

O modo recomendado para servidor usa dois servicos:

- `postgres`: PostgreSQL com volume persistente.
- `app`: container da pipeline para comandos pontuais e execucoes agendadas.

Prepare o `.env`:

```powershell
Copy-Item .env.example .env
```

Edite pelo menos:

```env
POSTGRES_DB=instagram_collector
POSTGRES_USER=collector
POSTGRES_PASSWORD=troque_esta_senha
TIMEZONE=America/Bahia
```

Suba o ambiente:

```powershell
docker compose up -d --build
```

Rode migrations e seed:

```powershell
docker compose run --rm app python -m pipeline migrate
docker compose run --rm app python -m pipeline seed-profiles
```

Comandos pontuais dentro do container:

```powershell
docker compose run --rm app python -m pipeline run-daily --skip-jobs
docker compose run --rm app python -m pipeline collect-posts --start-date 2026-07-01 --end-date 2026-07-23
docker compose run --rm app python -m pipeline collect-stories --date 2026-07-23
docker compose run --rm app python -m pipeline process-comments-queue --limit 100
docker compose run --rm app python -m pipeline export --date 2026-07-23
```

`collect-posts` coleta apenas publicações do feed/reels dentro do período informado. Para posts e stories no mesmo ciclo, use `run-daily`; para stories disponíveis naquele momento, use `collect-stories`.

Volumes do Compose:

```text
postgres_data  -> /var/lib/postgresql/data
media_data     -> /app/data
logs_data      -> /app/logs
reports_data   -> /app/reports
exports_data   -> /app/exports
```

Arquivos locais montados como somente leitura:

```text
profiles.json
sessions.json
cookies/
```

Para parar:

```powershell
docker compose down
```

Para apagar volumes persistentes, use apenas quando tiver certeza:

```powershell
docker compose down -v
```

## Ambiente local sem Docker

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
python -m pipeline migrate
python -m pipeline seed-profiles
```

## Perfis

Edite `profiles.json` para trocar o conjunto monitorado sem alterar codigo:

```json
[
  {
    "name": "Lula",
    "username": "lulaoficial",
    "active": true,
    "notes": "grupo presidencial",
    "priority": 10
  }
]
```

## Sessoes e rotacao

Crie `sessions.json` a partir de `sessions.example.json`:

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

Para habilitar rotacao:

```env
ACCOUNT_ROTATION_ENABLED=true
```

Os logs usam aliases anonimizados como `session-1a2b3c4d`. O pipeline nunca
imprime valores de cookies. Se uma sessao falhar, outra sessao ativa e tentada
uma vez, sem loop infinito.

## Comandos

Coleta diaria completa, posts do dia + stories disponiveis:

```powershell
python -m pipeline run-daily --skip-jobs
```

Coleta diaria de uma data especifica:

```powershell
python -m pipeline run-daily --date 2026-07-23 --skip-jobs
```

Somente posts:

```powershell
python -m pipeline run-daily --posts-only --skip-jobs
```

Somente stories:

```powershell
python -m pipeline run-daily --stories-only --skip-jobs
```

Coleta manual por periodo:

```powershell
python -m pipeline collect-posts --start-date 2026-07-01 --end-date 2026-07-23
```

Coleta separada de stories:

```powershell
python -m pipeline collect-stories --date 2026-07-23
```

Processar fila de comentarios:

```powershell
python -m pipeline process-comments-queue --limit 100 --rps 1
```

Exportar dados de um dia:

```powershell
python -m pipeline export --date 2026-07-23
```

## Banco e migrations

O schema versionado inicial fica em `migrations/001_init.sql`. O comando
operacional e:

```powershell
python -m pipeline migrate
```

No Docker:

```powershell
docker compose run --rm app python -m pipeline migrate
```

O PostgreSQL e a fonte principal. Arquivos locais guardam midias, logs,
relatorios e exports para auditoria fora do servidor.

## Saida

Responsabilidades:

- `data/`: arquivos brutos e midias baixadas.
- `logs/`: logs de execucao.
- `reports/`: relatorios JSON diarios.
- `exports/`: dados derivados para consulta externa.

Estrutura:

```text
data/raw/posts/YYYY-MM-DD/profile/posts.ndjson
data/raw/stories/YYYY-MM-DD/profile/
data/raw/comments/YYYY-MM-DD/profile/shortcode.ndjson
data/processed/posts_YYYY-MM-DD.csv
data/processed/stories_YYYY-MM-DD.csv
logs/YYYY-MM-DD.log
reports/YYYY-MM-DD.json
exports/YYYY-MM-DD/
```

O relatorio diario inclui perfis processados, status por perfil, erros, posts,
stories, jobs enfileirados, comentarios/replies inseridos, pendencias e arquivos.

## Limites da fila

Configure em `.env`:

```env
MAX_COMMENTS_PER_POST=500
JOB_LIMIT_PER_RUN=100
COMMENT_QUEUE_TIME_LIMIT_SECONDS=1800
MAX_JOB_ATTEMPTS=3
```

`MAX_COMMENTS_PER_POST` para a paginacao de comentarios ao atingir o limite,
evitando que um unico post trave a coleta.

## gallery-dl

O runner usa `include: ["stories"]`, `archive` para evitar duplicidade e
`sleep-request` conservador. O cookie pode vir de `cookies/collector-01.txt` ou,
se esse arquivo nao existir, do JSON da sessao configurada.

## Agendamento

O comando recomendado para servidor e cron e `run-scheduled`. Ele roda a coleta
diaria, pode exportar o dia e envia notificacao quando houver falha ou coleta
parcial.

Teste manual:

```bash
./scripts/run_daily_cron.sh
```

Ou diretamente:

```bash
docker compose run --rm app python -m pipeline run-scheduled --skip-jobs --export
```

Depois configure o cron Linux:

```cron
15 8 * * * cd /srv/instascraper && ./scripts/run_daily_cron.sh >> /srv/instascraper/logs/cron-daily.log 2>&1
*/30 * * * * cd /srv/instascraper && docker compose run --rm app python -m pipeline process-comments-queue --limit 50
30 23 * * * cd /srv/instascraper && docker compose run --rm app python -m pipeline export --date $(date +\%F)
```

Para stories, considere rodar mais de uma vez ao dia, pois stories expiram.

### Notificacoes

As notificacoes sao opcionais e configuradas por variaveis de ambiente. Por
padrao, o projeto notifica apenas falhas ou execucoes parciais. Para notificar
tambem sucesso, use `NOTIFY_ON_SUCCESS=true`.

Telegram recomendado para operar pelo celular:

```env
NOTIFY_ENABLED=true
NOTIFY_PROVIDER=telegram
NOTIFY_ON_SUCCESS=false
TELEGRAM_BOT_TOKEN=123456:token_do_bot
TELEGRAM_CHAT_ID=123456789
```

Email/SMTP como alternativa:

```env
NOTIFY_ENABLED=true
NOTIFY_PROVIDER=email
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=seu_email@gmail.com
SMTP_PASSWORD=senha_de_app
SMTP_FROM=seu_email@gmail.com
SMTP_TO=destino@exemplo.com
SMTP_USE_TLS=true
SMTP_USE_SSL=false
```

Para testar o alerta sem esperar erro real:

```bash
docker compose run --rm app python -m pipeline run-scheduled --skip-jobs --export --notify
```

## Backup

Backup do banco:

```powershell
docker compose exec postgres pg_dump -U $env:POSTGRES_USER $env:POSTGRES_DB > backup.sql
```

Backup dos volumes de midia/export/log deve ser feito no nivel do servidor ou
copiando os dados dos volumes Docker conforme a politica da infraestrutura.

## Cuidados

- Use RPS baixo.
- Nao versione `.env`, `sessions.json`, `cookies/` ou bancos locais.
- Rotacao deve usar contas autorizadas e auditaveis, nao servir para burlar bloqueios.
- Logs nao devem expor cookies, tokens ou senhas.
- Registre a finalidade academica, o periodo coletado e as contas institucionais usadas.
