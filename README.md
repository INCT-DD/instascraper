# Instagram Academic Collector

Pipeline leve para coleta academica de perfis publicos do Instagram.

- `instagram_scraper.py` preserva o fluxo interativo legado.
- `python -m pipeline ...` executa o pipeline nao interativo.
- `profiles.json` define os perfis monitorados.
- `sessions.json` define contas/cookies locais e nao deve ser versionado.
- `docker-compose.yml` sobe `app` + `postgres` para ambiente de servidor.

Inicio rapido com Docker:

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose run --rm app python -m pipeline migrate
docker compose run --rm app python -m pipeline seed-profiles
docker compose run --rm app python -m pipeline run-daily --skip-jobs
```

Para servidor com agendamento e alerta:

```bash
chmod +x scripts/run_daily_cron.sh
./scripts/run_daily_cron.sh
```

Leia [docs/pipeline.md](docs/pipeline.md) para instalacao completa, volumes,
migrations, rotacao, stories com `gallery-dl`, fila de comentarios, export,
backup, agendamento e notificacoes.
