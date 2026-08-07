# Instagram Academic Collector

Pipeline de coleta academica para monitoramento de perfis publicos do Instagram. O projeto foi estruturado para rodar de forma local ou em servidor, com PostgreSQL, Docker Compose, rotacao de contas por cookies, coleta de posts, stories via `gallery-dl`, fila de comentarios, exportacao de relatorios, logs operacionais e notificacoes.

O objetivo principal e permitir coleta recorrente e auditavel de metadados publicos para pesquisa. O codigo privilegia funcionamento leve, previsibilidade operacional e rastreabilidade dos dados coletados.

## Sumario

- [Visao geral](#visao-geral)
- [Arquitetura](#arquitetura)
- [Fluxos de coleta](#fluxos-de-coleta)
- [Coleta de posts em detalhe](#coleta-de-posts-em-detalhe)
- [Comandos principais](#comandos-principais)
- [Arquivos de configuracao](#arquivos-de-configuracao)
- [Docker e volumes](#docker-e-volumes)
- [Banco de dados](#banco-de-dados)
- [Rotas e fontes de dados](#rotas-e-fontes-de-dados)
- [Cookies e rotacao de contas](#cookies-e-rotacao-de-contas)
- [Stories e midias](#stories-e-midias)
- [Comentarios e fila de jobs](#comentarios-e-fila-de-jobs)
- [Agendamento com cron](#agendamento-com-cron)
- [Logs, relatorios e notificacoes](#logs-relatorios-e-notificacoes)
- [Exportacao](#exportacao)
- [Operacao em servidor](#operacao-em-servidor)
- [Limitacoes conhecidas](#limitacoes-conhecidas)
- [Cuidados academicos e seguranca](#cuidados-academicos-e-seguranca)
- [Troubleshooting](#troubleshooting)

## Visao Geral

O projeto coleta dados de perfis publicos definidos em `profiles.json`. A coleta pode ser executada manualmente, por periodo, diariamente ou por agendamento em servidor.

Principais capacidades:

- coleta de posts por perfil e intervalo de datas;
- coleta diaria de todos os perfis ativos;
- coleta de stories disponiveis no momento da execucao;
- persistencia em PostgreSQL;
- suporte a SQLite para testes locais simples;
- armazenamento de payload bruto em `raw_json` para auditoria;
- fila de comentarios e replies;
- exportacao de CSV/JSON para consulta externa;
- logs por data;
- notificacoes via Telegram ou email/SMTP;
- execucao em Docker Compose com volumes persistentes.

O projeto nao fornece interface web. A interface operacional e via CLI (`python -m pipeline ...`) e, em servidor, via cron ou scripts.

## Arquitetura

Estrutura principal:

```text
.
├─ instagram_scraper.py
├─ pipeline.py
├─ src/
│  └─ instagram_collector/
│     ├─ cli.py
│     ├─ config.py
│     ├─ files.py
│     ├─ gallerydl.py
│     ├─ jobs.py
│     ├─ logging_setup.py
│     ├─ notifications.py
│     ├─ pipeline.py
│     ├─ scraper.py
│     ├─ sessions.py
│     └─ storage.py
├─ migrations/
├─ scripts/
├─ docs/
├─ docker-compose.yml
├─ Dockerfile
├─ .env.example
├─ profile.example.json
└─ sessions.example.json
```

Responsabilidades dos modulos:

| Arquivo | Responsabilidade |
|---|---|
| `instagram_scraper.py` | Nucleo de scraping da v0: cookies, headers, rate limit, resolucao de `user_id`, paginas de posts, comentarios e parser basico. |
| `pipeline.py` | Wrapper para permitir `python -m pipeline ...` a partir da raiz do projeto. |
| `src/instagram_collector/cli.py` | Interface de linha de comando e roteamento dos comandos operacionais. |
| `src/instagram_collector/config.py` | Leitura de `.env`, perfis, sessoes e configuracoes gerais. |
| `src/instagram_collector/storage.py` | Acesso ao banco, schema, migrations simples, upserts e consultas. |
| `src/instagram_collector/pipeline.py` | Orquestracao da coleta diaria, coleta por periodo, seed de perfis e export. |
| `src/instagram_collector/scraper.py` | Adaptador nao interativo sobre o nucleo de scraping. |
| `src/instagram_collector/gallerydl.py` | Integracao com `gallery-dl` para stories e midias. |
| `src/instagram_collector/jobs.py` | Processamento da fila de comentarios e replies. |
| `src/instagram_collector/files.py` | Escrita de NDJSON, CSV, JSON, reports e exports. |
| `src/instagram_collector/logging_setup.py` | Configuracao de logs por data. |
| `src/instagram_collector/notifications.py` | Notificacao de runs por Telegram ou email/SMTP. |

Fluxo de alto nivel:

```text
profiles.json
   ↓
seed_profiles
   ↓
profiles no banco
   ↓
run-daily / collect-posts / collect-stories
   ↓
Instagram endpoints / gallery-dl
   ↓
posts, stories, raw_payloads, jobs
   ↓
reports, logs, exports e notificacoes
```

## Fluxos de Coleta

### Coleta diaria

O comando diario percorre todos os perfis ativos:

```powershell
docker compose run --rm app python -m pipeline run-daily --date 2026-08-07 --skip-jobs --rps 0.5
```

Por padrao, `run-daily` tenta posts e stories. Para apenas posts:

```powershell
docker compose run --rm app python -m pipeline run-daily --date 2026-08-07 --posts-only --skip-jobs --rps 0.5
```

Para apenas stories:

```powershell
docker compose run --rm app python -m pipeline run-daily --date 2026-08-07 --stories-only --skip-jobs
```

### Coleta manual por periodo

Coleta posts de todos os perfis ativos em um intervalo:

```powershell
docker compose run --rm app python -m pipeline collect-posts --start-date 2026-06-01 --end-date 2026-08-05 --no-comments --rps 0.5
```

Coleta posts de apenas um perfil:

```powershell
docker compose run --rm app python -m pipeline collect-posts --username lulaoficial --start-date 2026-08-07 --end-date 2026-08-07 --no-comments --rps 0.5
```

### Coleta manual de stories

```powershell
docker compose run --rm app python -m pipeline collect-stories --date 2026-08-07
```

Ou para um perfil:

```powershell
docker compose run --rm app python -m pipeline collect-stories --date 2026-08-07 --username lulaoficial
```

## Coleta de Posts em Detalhe

A coleta de posts foi desenhada para monitorar perfis publicos definidos na base de dados, respeitando um intervalo de datas e mantendo rastreabilidade do que foi retornado pelo Instagram. Ela nao usa a API oficial da Meta. A v0 trabalha com endpoints web/privados do Instagram, autenticados por cookies de contas coletoras.

### Entrada da coleta

O ponto de partida e a lista de perfis ativos. Normalmente essa lista vem de `profiles.json` e e sincronizada para a tabela `profiles` pelo comando:

```powershell
docker compose run --rm app python -m pipeline seed-profiles
```

Depois disso, os comandos `run-daily` e `collect-posts` consultam o banco e percorrem os perfis ativos. Em uma coleta manual, tambem e possivel restringir para um unico perfil com `--username`.

### Escolha da sessao e cookies

Antes de consultar o Instagram, a aplicacao escolhe uma sessao de coleta. Cada sessao representa uma conta autenticada por cookies e fica descrita em `sessions.json`.

Quando `ACCOUNT_ROTATION_ENABLED=true`, o projeto usa o pool de sessoes configurado. A ideia e distribuir as requisicoes entre contas e permitir que a coleta tente outra sessao quando uma conta apresentar falha de autenticacao, bloqueio temporario ou challenge.

Para posts, o arquivo principal de cookie e o JSON indicado em `instagram_cookie_json`, normalmente dentro da pasta `cookies/`:

```json
{
  "sessionid": "...",
  "csrftoken": "...",
  "mid": "...",
  "ds_user_id": "..."
}
```

Esses cookies sao usados para montar as requisicoes HTTP ao Instagram. A conta nao precisa ficar aberta no navegador do servidor depois que os cookies foram exportados, mas os cookies precisam continuar validos. Se a conta for deslogada em todos os dispositivos, se trocar senha, se houver challenge, bloqueio ou expiracao de sessao, o cookie pode deixar de funcionar e deve ser renovado.

### Resolucao do `user_id`

O Instagram pagina posts pelo identificador numerico do perfil, nao apenas pelo `username`. Por isso, antes de buscar o feed, o scraper tenta resolver o `user_id`.

Primeiro ele consulta:

```text
/api/v1/users/web_profile_info/?username={username}
```

Em alguns perfis, esse endpoint pode retornar `HTTP 400` mesmo com cookies validos. Isso nao encerra a coleta imediatamente. A v0 tenta um fallback:

```text
/web/search/topsearch/?query={username}
```

O fallback procura o perfil pelo username e extrai o `pk`/`user_id` do resultado correspondente. Por isso e possivel aparecer no log um erro em `web_profile_info` e, ainda assim, os posts serem coletados normalmente.

### Paginacao do feed

Com o `user_id` resolvido, a coleta busca paginas do feed do perfil por:

```text
/api/v1/feed/user/{user_id}/?count=12
```

Cada pagina retorna uma lista de itens e, quando existem mais itens disponiveis, um cursor de continuacao. A v0 usa o cursor `next_max_id` como `max_id` na proxima requisicao. Esse processo continua enquanto:

- houver mais paginas;
- ainda existirem posts dentro ou possivelmente dentro do periodo solicitado;
- nao ocorrer uma falha de autenticacao, bloqueio ou limite de requisicoes.

O parametro `--rps` controla a velocidade das requisicoes. Por exemplo, `--rps 0.5` significa aproximadamente uma requisicao a cada dois segundos. Para monitoramento academico recorrente, valores baixos sao preferiveis porque reduzem chance de bloqueio e tornam a coleta mais estavel.

### Filtro por periodo

Os posts retornam com timestamp Unix, geralmente em `taken_at`. O scraper converte esse valor para data/hora UTC e compara com o intervalo solicitado.

Na coleta por periodo, `--start-date` e `--end-date` definem o intervalo de interesse. Na coleta diaria, `run-daily --date AAAA-MM-DD` monta a janela do dia considerando a variavel `TIMEZONE`. Isso e importante porque o banco pode guardar `taken_at_iso` em UTC, enquanto o dia operacional pode ser America/Bahia.

Durante a paginacao:

- posts mais novos que o fim do periodo sao ignorados;
- posts dentro do periodo sao normalizados e persistidos;
- posts mais antigos que o inicio do periodo sao ignorados;
- quando a pagina inteira ja esta antes do inicio do periodo, a coleta para aquele perfil pode ser encerrada, evitando buscar paginas antigas desnecessariamente.

Esse comportamento explica por que um perfil pode retornar zero posts em um dia especifico: se nao houve publicacao dentro da janela calculada, nada sera persistido, mesmo que o perfil tenha posts antes ou depois da data.

### Normalizacao dos dados

O Instagram pode retornar estruturas diferentes dependendo do tipo de midia, do perfil, do endpoint e de mudancas internas da plataforma. Por isso a v0 transforma cada item em um formato interno antes de salvar.

Na normalizacao, o projeto tenta extrair:

- identificador da publicacao (`id`/`pk`);
- `shortcode`;
- URL publica do post;
- data de publicacao (`taken_at` e `taken_at_iso`);
- tipo de midia;
- legenda;
- numero de curtidas;
- numero de comentarios;
- visualizacoes quando houver;
- sinalizacao de video;
- texto de acessibilidade quando houver;
- contagem de reposts/compartilhamentos quando o payload expuser essa metrica;
- payload bruto em `raw_json`.

O campo `reposts` merece cuidado metodologico. A aplicacao procura chaves como `media_repost_count`, `share_count`, `reshare_count`, `repost_count`, `reposts_count` e variacoes semelhantes. Se nenhuma dessas chaves vier no payload, o banco salva `NULL`. Isso significa "metrica nao disponibilizada pelo Instagram neste payload", e nao "zero reposts".

### Persistencia

Depois da normalizacao, cada post e salvo na tabela `posts`. A persistencia usa upsert: se a publicacao ainda nao existe, ela e criada; se ja existe, os campos conhecidos sao atualizados.

A relacao com o perfil e preservada por `profile_id`. Assim, mesmo com todos os posts em uma unica tabela, e possivel filtrar por candidato/perfil usando SQL:

```sql
SELECT pr.username, p.shortcode, p.taken_at_iso, p.likes, p.comments_count, p.reposts
FROM posts p
JOIN profiles pr ON pr.id = p.profile_id
WHERE pr.username = 'lulaoficial'
ORDER BY p.taken_at_iso DESC;
```

O payload bruto tambem e preservado em `posts.raw_json` e pode ser registrado em `raw_payloads`. Isso ajuda auditoria academica, reproducibilidade parcial da interpretacao e investigacao quando o Instagram altera nomes de campos ou formatos de resposta.

### Comentarios

A coleta de posts e a coleta de comentarios sao separaveis.

Quando `collect-posts` e executado com `--no-comments`, os posts sao coletados sem enfileirar/processar comentarios. Quando comentarios estao habilitados, posts com `comments_count > 0` podem gerar jobs para processamento posterior.

No `run-daily`, a flag `--skip-jobs` significa que a coleta diaria nao processara a fila de comentarios naquela execucao. Ela e util quando o objetivo do dia e coletar apenas posts e stories, deixando comentarios para outro horario ou outro comando.

### Falhas esperadas e interpretacao dos logs

Alguns erros nao significam necessariamente perda da coleta:

- `HTTP 400` em `web_profile_info`: pode ser contornado pelo fallback `topsearch`;
- `HTTP 302` para `/`, `/login` ou `/challenge`: normalmente indica cookie invalido, expirado ou conta em verificacao;
- resposta HTML em rota de API: costuma indicar redirecionamento, challenge ou sessao invalida;
- `HTTP 401` ou `HTTP 403`: geralmente indica falha de autenticacao ou permissao;
- `HTTP 429`: indica limite de requisicoes, exigindo reduzir `--rps`, aguardar ou trocar sessao;
- zero posts no intervalo: pode ser resultado correto se nao houve publicacao dentro da janela de datas.

## Comandos Principais

| Comando | Uso |
|---|---|
| `migrate` | Cria ou atualiza schema do banco. Deve ser usado na primeira execucao, apos zerar volumes ou quando houver mudanca de schema. |
| `seed-profiles` | Sincroniza `profiles.json` com a tabela `profiles`. |
| `run-daily` | Executa a coleta diaria de perfis ativos. |
| `run-scheduled` | Executa a coleta diaria com comportamento voltado para cron: pode exportar e notificar falhas. |
| `collect-posts` | Coleta posts por periodo, com opcao de filtrar por perfil. |
| `collect-stories` | Coleta stories disponiveis no momento da execucao. |
| `process-comments-queue` | Processa a fila pendente de comentarios/replies. |
| `process-jobs` | Alias operacional para processamento da fila. |
| `export` | Gera exportacao do dia em pasta propria. |
| `collect-profile` | Alias de compatibilidade para coleta de perfil especifico. |

Ajuda dos comandos:

```powershell
docker compose run --rm app python -m pipeline --help
docker compose run --rm app python -m pipeline run-daily --help
docker compose run --rm app python -m pipeline collect-posts --help
```

## Arquivos de Configuracao

### `.env`

Arquivo local com variaveis de ambiente. Nao deve ser versionado.

Crie a partir do exemplo:

```powershell
Copy-Item .env.example .env
```

Variaveis importantes:

| Variavel | Descricao |
|---|---|
| `POSTGRES_DB` | Nome do banco. |
| `POSTGRES_USER` | Usuario do PostgreSQL. |
| `POSTGRES_PASSWORD` | Senha do PostgreSQL. |
| `POSTGRES_HOST` | Host do banco dentro do Docker. Normalmente `postgres`. |
| `POSTGRES_PORT` | Porta interna do PostgreSQL. |
| `TIMEZONE` | Timezone usada para janela diaria. Exemplo: `America/Bahia`. |
| `RPS` | Requisicoes por segundo. Use valores conservadores, como `0.2` a `0.5`. |
| `MARGIN_DAYS` | Dias adicionais para tras na coleta diaria. |
| `ACCOUNT_ROTATION_ENABLED` | Ativa rotacao entre sessoes do `sessions.json`. |
| `GALLERY_DL_ENABLED` | Ativa ou desativa coleta de stories via `gallery-dl`. |
| `NOTIFY_ENABLED` | Ativa notificacoes para `run-scheduled`. |
| `NOTIFY_PROVIDER` | `telegram` ou `email`. |

### `profiles.json`

Arquivo local com a lista real de perfis monitorados. Ele deve ficar fora do Git.

Crie a partir do exemplo versionado:

```powershell
Copy-Item profile.example.json profiles.json
```

Formato:

```json
[
  {
    "name": "Nome do Perfil",
    "username": "usuariosemarroba",
    "active": true,
    "notes": "Observacao academica ou criterio de inclusao.",
    "priority": 10
  }
]
```

Campos:

| Campo | Descricao |
|---|---|
| `name` | Nome legivel do perfil. |
| `username` | Username sem `@`. |
| `active` | Define se o perfil entra na coleta. |
| `notes` | Campo livre para observacoes metodologicas. |
| `priority` | Prioridade de coleta. Perfis com maior prioridade aparecem primeiro. |

### `sessions.json`

Arquivo local com contas coletoras e caminhos dos cookies. Nao deve ser versionado.

Crie a partir do exemplo:

```powershell
Copy-Item sessions.example.json sessions.json
```

Formato:

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

O campo `gallery_dl_cookies` pode apontar para um cookies.txt no formato Netscape. Se o arquivo `.txt` nao for valido, o coletor tenta usar o JSON da sessao.

### `cookies/`

Pasta local com cookies das contas coletoras. Nao deve ser versionada.

Exemplo:

```text
cookies/
├─ collector-01.json
├─ collector-01.txt
├─ collector-02.json
└─ collector-02.txt
```

## Docker e Volumes

O `docker-compose.yml` define dois servicos:

| Servico | Responsabilidade |
|---|---|
| `postgres` | Banco PostgreSQL persistente. |
| `app` | Ambiente Python da pipeline. |

Volumes:

| Volume | Montagem no container | Conteudo |
|---|---|---|
| `postgres_data` | `/var/lib/postgresql/data` | Dados persistentes do PostgreSQL. |
| `media_data` | `/app/data` | Dados brutos, posts NDJSON, stories e midias. |
| `logs_data` | `/app/logs` | Logs da aplicacao. |
| `reports_data` | `/app/reports` | Relatorios JSON diarios. |
| `exports_data` | `/app/exports` | Exportacoes para consulta externa. |

Arquivos locais montados como somente leitura:

```text
./profiles.json  -> /app/profiles.json
./sessions.json  -> /app/sessions.json
./cookies        -> /app/cookies
```

Subir ambiente:

```powershell
docker compose up -d --build
```

Rodar migrations:

```powershell
docker compose run --rm app python -m pipeline migrate
```

Sincronizar perfis:

```powershell
docker compose run --rm app python -m pipeline seed-profiles
```

## Banco de Dados

O schema principal fica em `src/instagram_collector/storage.py` e `migrations/`.

Tabelas principais:

| Tabela | Descricao |
|---|---|
| `profiles` | Perfis monitorados. |
| `collection_runs` | Execucoes gerais da pipeline. |
| `profile_collection_status` | Status por perfil em uma execucao. |
| `posts` | Posts coletados e metricas normalizadas. |
| `stories` | Stories coletados e caminho de midia. |
| `comments` | Comentarios coletados. |
| `replies` | Replies de comentarios. |
| `collection_jobs` | Fila de jobs de comentarios/replies. |
| `raw_payloads` | Payloads brutos para auditoria. |

Campos relevantes de `posts`:

| Campo | Descricao |
|---|---|
| `profile_id` | Referencia ao perfil. |
| `platform_post_id` | ID do post na plataforma. |
| `shortcode` | Shortcode usado na URL `/p/{shortcode}/`. |
| `taken_at_iso` | Data/hora normalizada em ISO. |
| `media_type` | `photo`, `video`, `carousel` ou `unknown`. |
| `caption` | Legenda. |
| `likes` | Curtidas quando disponivel. |
| `comments_count` | Contagem de comentarios quando disponivel. |
| `reposts` | Contagem de reposts/compartilhamentos quando o payload expuser a metrica. Pode ser `NULL`. |
| `views` | Visualizacoes quando disponivel. |
| `raw_json` | Payload bruto normalizado/salvo para auditoria. |

Consulta por perfil:

```sql
SELECT pr.username, p.shortcode, p.taken_at_iso, p.likes, p.comments_count, p.reposts
FROM posts p
JOIN profiles pr ON pr.id = p.profile_id
WHERE pr.username = 'lulaoficial'
ORDER BY p.taken_at_iso DESC;
```

Status da ultima coleta:

```sql
SELECT handle, status, posts_found, stories_found, error_message
FROM profile_collection_status
WHERE run_id = (
  SELECT max(id)
  FROM collection_runs
  WHERE run_type = 'daily'
)
ORDER BY handle;
```

## Rotas e Fontes de Dados

Este projeto nao usa API oficial da Meta para perfis de terceiros. A coleta atual usa endpoints web/privados do Instagram autenticados por cookies.

Rotas usadas para posts:

| Rota | Finalidade |
|---|---|
| `/api/v1/users/web_profile_info/?username={username}` | Tenta obter o `user_id` numerico do perfil. |
| `/web/search/topsearch/?query={username}` | Fallback para obter `user_id` quando `web_profile_info` falha. |
| `/api/v1/feed/user/{user_id}/?count=12` | Pagina posts do perfil por `user_id`. |
| `/graphql/query/` | Usado para comentarios e replies via query hash. |

Rotas e ferramentas para stories:

| Fonte | Finalidade |
|---|---|
| `gallery-dl` com extractor `instagram` | Coleta stories disponiveis no momento. |
| Cookies da sessao | Autenticacao para o extractor. |

Observacoes:

- `HTTP 400` em `web_profile_info` pode ocorrer em perfis especificos. O fallback `topsearch` normalmente resolve.
- `HTTP 302` para `/`, `/login` ou `/challenge` indica problema de sessao/cookie/verificacao.
- Resposta HTML em endpoint de API indica redirecionamento, challenge ou sessao invalida.
- Campos como `reposts` dependem do payload entregue pelo Instagram. A ausencia do campo nao significa zero.

## Cookies e Rotacao de Contas

Posts usam preferencialmente cookies em JSON:

```json
{
  "sessionid": "...",
  "csrftoken": "...",
  "mid": "...",
  "ds_user_id": "..."
}
```

Stories via `gallery-dl` podem usar cookies Netscape `.txt` ou o JSON da sessao. Um arquivo `.txt` precisa ter linhas com 7 colunas separadas por tabulacao. Se ele for apenas JSON renomeado para `.txt`, o projeto ignora esse `.txt` e tenta o JSON.

Rotacao:

```env
ACCOUNT_ROTATION_ENABLED=true
```

Com `sessions.json`:

```json
[
  {
    "name": "collector-01",
    "active": true,
    "instagram_cookie_json": "cookies/collector-01.json",
    "gallery_dl_cookies": "cookies/collector-01.txt"
  },
  {
    "name": "collector-02",
    "active": true,
    "instagram_cookie_json": "cookies/collector-02.json",
    "gallery_dl_cookies": "cookies/collector-02.txt"
  }
]
```

Cuidados:

- nao versionar cookies;
- nao clicar em "sair" da conta depois de extrair cookies;
- renovar cookies quando houver `401`, `302`, `/challenge` ou HTML inesperado;
- usar contas autorizadas e auditaveis para a pesquisa;
- registrar quais contas coletoras foram usadas.

## Stories e Midias

Stories sao coletados via `gallery-dl`. As midias ficam em:

```text
/app/data/raw/stories/YYYY-MM-DD/username/
```

No codigo, isso corresponde a:

```text
data/raw/stories/YYYY-MM-DD/username/
```

Como o Docker usa volume nomeado, esses arquivos nao aparecem diretamente na pasta do projeto no Windows. Para copiar para o PC:

```powershell
docker compose up -d app
docker cp instascraper-app-1:/app/data/raw/stories .\stories
```

Se preferir acesso direto pelo Windows, troque no `docker-compose.yml` o volume nomeado:

```yaml
- media_data:/app/data
```

por um bind mount local:

```yaml
- ./data:/app/data
```

Nesse caso, os arquivos aparecerao em:

```text
C:\Users\Ricardo\OneDrive\Documentos\instascraper\data\raw\stories
```

## Comentarios e Fila de Jobs

O pipeline separa coleta de posts e processamento de comentarios.

No `run-daily`, posts podem enfileirar jobs de comentarios. O parametro `--skip-jobs` impede o processamento da fila no final da execucao.

Com `--skip-jobs`:

```text
coleta posts
coleta stories
enfileira jobs de comentarios
nao processa comentarios
```

Sem `--skip-jobs`:

```text
coleta posts
coleta stories
enfileira jobs de comentarios
processa comentarios e replies no final
```

Processar fila manualmente:

```powershell
docker compose run --rm app python -m pipeline process-comments-queue --limit 100 --rps 0.5
```

Coletar posts sem sequer enfileirar comentarios:

```powershell
docker compose run --rm app python -m pipeline collect-posts --start-date 2026-08-07 --end-date 2026-08-07 --no-comments --rps 0.5
```

## Agendamento com Cron

O comando recomendado para servidor e cron e:

```bash
./scripts/run_daily_cron.sh
```

Ele executa:

```bash
docker compose run --rm app python -m pipeline run-scheduled --skip-jobs --export
```

Tornar executavel no Linux:

```bash
chmod +x scripts/run_daily_cron.sh
```

Editar cron:

```bash
crontab -e
```

Exemplo diario as 08:15:

```cron
15 8 * * * cd /srv/instascraper && ./scripts/run_daily_cron.sh >> /srv/instascraper/logs/cron-daily.log 2>&1
```

Exemplo com fila de comentarios:

```cron
15 8 * * * cd /srv/instascraper && ./scripts/run_daily_cron.sh >> /srv/instascraper/logs/cron-daily.log 2>&1
45 8 * * * cd /srv/instascraper && docker compose run --rm app python -m pipeline process-comments-queue --limit 100 --rps 0.5 >> /srv/instascraper/logs/cron-comments.log 2>&1
```

Listar cron:

```bash
crontab -l
```

Ver logs:

```bash
tail -n 100 /srv/instascraper/logs/cron-daily.log
```

## Logs, Relatorios e Notificacoes

Logs da aplicacao:

```text
logs/YYYY-MM-DD.log
```

Relatorio diario:

```text
reports/YYYY-MM-DD.json
```

O comando `run-scheduled` pode enviar notificacoes quando houver falha ou execucao parcial.

### Telegram

Configuracao:

```env
NOTIFY_ENABLED=true
NOTIFY_PROVIDER=telegram
NOTIFY_ON_SUCCESS=false
TELEGRAM_BOT_TOKEN=123456:token_do_bot
TELEGRAM_CHAT_ID=123456789
```

Teste forçando notificacao:

```powershell
docker compose run --rm app python -m pipeline run-scheduled --date 2026-08-07 --skip-jobs --export --notify
```

### Email/SMTP

Configuracao:

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

Recomendacao: para Gmail, use senha de app, nao a senha principal da conta.

## Exportacao

Exportar um dia:

```powershell
docker compose run --rm app python -m pipeline export --date 2026-08-07
```

Saida:

```text
exports/YYYY-MM-DD/
```

O export inclui relatorio, dados brutos do dia e CSVs processados quando gerados.

## Operacao em Servidor

Fluxo recomendado para servidor fisico:

1. Instalar Linux, Docker e Docker Compose plugin.
2. Clonar ou copiar o projeto para `/srv/instascraper` ou `/opt/instascraper`.
3. Criar `.env`, `profiles.json`, `sessions.json` e `cookies/`.
4. Subir containers.
5. Rodar `migrate`.
6. Rodar `seed-profiles`.
7. Testar um perfil.
8. Agendar cron.
9. Ativar notificacoes.
10. Configurar backup.

Comandos:

```bash
cd /srv/instascraper
cp .env.example .env
cp profile.example.json profiles.json
cp sessions.example.json sessions.json
docker compose up -d --build
docker compose run --rm app python -m pipeline migrate
docker compose run --rm app python -m pipeline seed-profiles
docker compose run --rm app python -m pipeline collect-posts --username lulaoficial --start-date 2026-08-07 --end-date 2026-08-07 --no-comments --rps 0.5
```

Atualizacao de codigo no servidor:

```bash
cd /srv/instascraper
git pull
docker compose build app
docker compose run --rm app python -m pipeline migrate
docker compose up -d app
```

Se a mudanca nao envolver banco, `migrate` normalmente nao e necessario.

## Limitacoes Conhecidas

### Reposts e compartilhamentos

O campo `posts.reposts` depende do payload retornado pelo Instagram. Historicamente, alguns payloads expuseram `media_repost_count`. Em coletas recentes, esse campo pode nao vir mais. Quando a metrica nao aparece no payload, o projeto salva `NULL`.

Interpretacao correta:

```text
NULL = metrica indisponivel no payload
0    = metrica veio explicitamente como zero
```

### Stories

Stories expiram. Uma coleta diaria pode perder stories publicados e removidos entre execucoes. Se stories forem centrais para a pesquisa, execute a coleta mais de uma vez por dia.

### Cookies

Cookies podem expirar, cair em challenge ou ser invalidados por logout, troca de senha ou verificacao de seguranca.

### Endpoints privados

As rotas usadas nao sao contrato oficial estavel. Podem mudar sem aviso.

Para dados academicos institucionalmente mais robustos, avalie Meta Content Library/API quando aplicavel.

## Cuidados Academicos e Seguranca

Recomendacoes:

- documentar o criterio de inclusao de perfis;
- registrar periodo de coleta;
- preservar `raw_json` quando necessario para auditoria;
- separar metadados de midias pesadas;
- usar logs e relatorios de execucao;
- nao versionar cookies, `.env`, `sessions.json`, `profiles.json` ou dados coletados;
- proteger arquivos de cookies como segredo;
- evitar RPS alto;
- manter backups do banco;
- documentar falhas, lacunas e indisponibilidade de metricas.

Arquivos que nao devem ser versionados:

```text
.env
profiles.json
sessions.json
cookies/
data/
logs/
reports/
exports/
collector.db
```

## Troubleshooting

### `HTTP 400` em `web_profile_info`

Pode ser esperado. O scraper tenta fallback via `topsearch`.

Se o fallback retorna `200 OK` e depois posts sao coletados, nao e problema.

### `Instagram redirecionou feed/user para /`

Indica cookies invalidos, expirados ou conta em verificacao.

Acao:

1. entrar no Instagram com a conta coletora;
2. resolver challenge/verificacao;
3. exportar cookies novamente;
4. substituir arquivo em `cookies/`;
5. testar um perfil.

### `Invalid Netscape cookies.txt file`

O `gallery-dl` recebeu um `.txt` que nao esta no formato Netscape. O projeto tenta cair para JSON quando detecta isso. Se persistir, gere um cookies.txt valido ou remova a referencia ao `.txt` no `sessions.json`.

### `0 story files`

Possibilidades:

- perfil nao possui stories disponiveis;
- story expirou;
- cookie do `gallery-dl` invalido;
- `GALLERY_DL_ENABLED=false`;
- `gallery-dl` nao instalado na imagem/ambiente.

### `profiles.json` ausente

Crie a partir do exemplo:

```powershell
Copy-Item profile.example.json profiles.json
```

No Linux:

```bash
cp profile.example.json profiles.json
```

### Quando rodar `migrate`

Rode `migrate`:

- primeira execucao;
- apos `docker compose down -v`;
- quando houver alteracao de schema;
- quando a documentacao de uma atualizacao indicar.

Nao precisa rodar `migrate` para cada coleta diaria.
