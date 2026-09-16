# Referência da interface de linha de comando

Esta é a referência operacional de todos os subcomandos expostos por `python -m pipeline`. As datas utilizam o formato ISO `AAAA-MM-DD`.

## Formas de execução

Com Docker, recomendado:

```powershell
docker compose run --rm app python -m pipeline <comando> [opções]
```

Em ambiente Python instalado:

```bash
python -m pipeline <comando> [opções]
instagram-collector <comando> [opções]
```

Ajuda geral ou específica:

`-h` e `--help` estão disponíveis no programa principal e em todos os subcomandos. Eles exibem a sintaxe aplicável e encerram sem executar operações.

```powershell
docker compose run --rm app python -m pipeline --help
docker compose run --rm app python -m pipeline collect-posts --help
```

## Convenções

- `--start-date` e `--end-date` são inclusivos para o usuário.
- `--date` usa a data atual do fuso `TIMEZONE` quando omitido.
- `--rps` deve ser finito e maior que zero. Ele representa requisições por segundo; `0.2` corresponde a aproximadamente uma requisição a cada cinco segundos.
- `--username` aceita o nome com ou sem `@`.
- Jobs de mídia são independentes da fila de comentários.
- Comandos que encontram falhas operacionais retornam código `1`; sucesso retorna `0`.

## Índice de comandos

| Comando | Finalidade |
|---|---|
| `run-daily` | Coleta diária de posts e stories. |
| `run-scheduled` | Coleta diária para cron, com export e notificação opcionais. |
| `collect-posts` | Coleta posts de um período. |
| `collect-stories` | Coleta ou enfileira stories disponíveis. |
| `collect-profile` | Alias de compatibilidade para coleta de um perfil. |
| `refresh-post-metrics` | Atualiza métricas de posts existentes. |
| `refresh-failed-media` | Renova URLs de mídias falhadas. |
| `process-media-queue` | Processa mídias de posts e stories. |
| `process-jobs` | Processa comentários e respostas. |
| `process-comments-queue` | Alias explícito de `process-jobs`. |
| `export` | Gera export de uma data. |
| `seed-profiles` | Sincroniza perfis configurados com o banco. |
| `init-db` | Cria ou atualiza o schema embarcado. |
| `migrate` | Executa migrations versionadas. |
| `cleanup-secondary-data` | Audita ou remove dados secundários selecionados. |

## run-daily

Coleta todos os perfis ativos para uma data de referência. Sem filtros de modo, respeita `COLLECT_POSTS_DEFAULT` e `COLLECT_STORIES_DEFAULT`.

```text
run-daily [--date DATA] [--margin-days N] [--rps RPS]
          [--skip-jobs] [--posts-only | --stories-only]
          [--retry-incomplete] [--new-only]
```

| Opção | Comportamento |
|---|---|
| `--date` | Data alvo. Padrão: hoje no fuso configurado. |
| `--margin-days` | Amplia a janela de posts para dias anteriores à data alvo. |
| `--rps` | Sobrescreve `RPS` para a coleta de posts. |
| `--skip-jobs` | Não processa comentários/respostas ao final. Não para o worker de mídia. |
| `--posts-only` | Desativa stories nessa execução. |
| `--stories-only` | Desativa posts nessa execução. |
| `--retry-incomplete` | Seleciona perfis sem conclusão válida para a data. |
| `--new-only` | Para a paginação de posts no primeiro post regular já armazenado. Não afeta stories. |

`--posts-only` e `--stories-only` não podem ser combinados.

Exemplo:

```powershell
docker compose run --rm app python -m pipeline run-daily --date 2026-09-15 --new-only --skip-jobs --rps 0.2
```

## run-scheduled

Executa o mesmo fluxo de `run-daily`, com recursos de cron, export e notificação.

```text
run-scheduled [opções de run-daily] [--export]
              [--notify | --no-notify]
```

Opções adicionais:

| Opção | Comportamento |
|---|---|
| `--export` | Gera o export da data após a coleta. |
| `--notify` | Força tentativa de notificação mesmo se `NOTIFY_ENABLED=false`. |
| `--no-notify` | Impede notificação nessa execução. |

`--notify` e `--no-notify` representam intenções opostas e não devem ser combinados.

```bash
./scripts/run_daily_cron.sh --date 2026-09-15 --new-only
```

## collect-posts

Coleta posts de todos os perfis ativos ou de um perfil específico.

```text
collect-posts --start-date DATA --end-date DATA
              [--username PERFIL] [--no-comments] [--rps RPS]
              [--new-only] [--resume | --retry-failed]
```

| Opção | Comportamento |
|---|---|
| `--start-date` | Primeiro dia do intervalo, obrigatório. |
| `--end-date` | Último dia do intervalo, obrigatório. |
| `--username` | Restringe a um perfil. |
| `--no-comments` | Não cria jobs de comentários. |
| `--rps` | Taxa compartilhada entre perfis da execução. |
| `--new-only` | Evita percorrer páginas anteriores ao primeiro post regular conhecido. |
| `--resume` | Refaz perfis incompletos ou não tentados no intervalo exato. |
| `--retry-failed` | Refaz apenas perfis cuja tentativa mais recente falhou no intervalo exato. |

`--resume` e `--retry-failed` são mutuamente exclusivos. Eles selecionam perfis, mas não retomam um cursor: cada perfil selecionado começa novamente da primeira página.

Exemplo incremental sem comentários:

```powershell
docker compose run --rm app python -m pipeline collect-posts --start-date 2026-09-01 --end-date 2026-09-15 --no-comments --new-only --rps 0.2
```

## collect-stories

Consulta os stories disponíveis para todos os perfis ativos ou para um perfil.

```text
collect-stories [--date DATA] [--username PERFIL]
```

| Opção | Comportamento |
|---|---|
| `--date` | Data usada na organização e identificação do job. Padrão: hoje. |
| `--username` | Restringe a um perfil. |

Com `MEDIA_QUEUE_ENABLED=true`, o comando apenas cria jobs prioritários. Sem fila, executa o `gallery-dl` no próprio processo. A data não permite recuperar stories expirados de um dia anterior.

```powershell
docker compose run --rm app python -m pipeline collect-stories --date 2026-09-15 --username lulaoficial
```

## collect-profile

Alias de compatibilidade para coleta de posts de um único perfil.

```text
collect-profile --username PERFIL --from DATA --to DATA
                [--comments] [--rps RPS] [--new-only]
```

| Opção | Comportamento |
|---|---|
| `--username` | Perfil obrigatório. |
| `--from` | Primeiro dia inclusivo. |
| `--to` | Último dia inclusivo. |
| `--comments` | Cria e processa jobs de comentários se a funcionalidade estiver habilitada. |
| `--rps` | Taxa da coleta e, quando aplicável, dos comentários. |
| `--new-only` | Usa o limite incremental de posts conhecidos. |

Para novos scripts, prefira `collect-posts --username`.

## refresh-post-metrics

Atualiza contadores dos posts já existentes por meio do endpoint de detalhe da mídia.

```text
refresh-post-metrics --start-date DATA --end-date DATA
                     [--username PERFIL] [--limit N] [--rps RPS]
```

| Opção | Comportamento |
|---|---|
| `--start-date` | Primeiro dia dos posts selecionados. |
| `--end-date` | Último dia dos posts selecionados. |
| `--username` | Restringe a um perfil. |
| `--limit` | Máximo de posts, em ordem dos mais recentes. Deve ser maior que zero. |
| `--rps` | Taxa das consultas individuais de detalhe. |

O comando realiza aproximadamente uma requisição por post. Atualiza `likes`, `comments_count`, `views` e `reposts`. Valores ausentes não sobrescrevem métricas existentes; zero sobrescreve. Cada resposta bem-sucedida é auditada em `raw_payloads`.

```powershell
docker compose run --rm app python -m pipeline refresh-post-metrics --start-date 2026-09-01 --end-date 2026-09-15 --limit 100 --rps 0.1
```

## refresh-failed-media

Renova URLs temporárias apenas para posts que possuem mídia falhada ou em retry.

```text
refresh-failed-media --start-date DATA --end-date DATA [--limit N]
```

| Opção | Comportamento |
|---|---|
| `--start-date` | Primeiro dia dos posts afetados. |
| `--end-date` | Último dia dos posts afetados. |
| `--limit` | Máximo de posts a renovar. |

Após a renovação, mantenha ou inicie o `media-worker` para consumir os jobs reabertos.

## process-media-queue

Processa a fila de mídias de posts e stories.

```text
process-media-queue [--limit N] [--watch] [--sleep SEGUNDOS]
```

| Opção | Comportamento |
|---|---|
| `--limit` | Máximo de jobs processados por ciclo. Sem valor, usa a política interna/configurada. |
| `--watch` | Mantém o processo consultando a fila continuamente. |
| `--sleep` | Espera entre ciclos quando `--watch` está ativo. |

No Compose, o serviço `media-worker` já executa esse comando em modo contínuo.

```powershell
docker compose up -d media-worker
docker compose logs -f media-worker
```

## process-jobs e process-comments-queue

Os dois comandos executam o mesmo processador de comentários e respostas.

```text
process-comments-queue [--limit N] [--rps RPS]
process-jobs           [--limit N] [--rps RPS]
```

| Opção | Comportamento |
|---|---|
| `--limit` | Máximo de jobs no ciclo. |
| `--rps` | Taxa das requisições de comentários. |

Se `COLLECT_COMMENTS_DEFAULT=false`, o comando informa que a coleta está desabilitada e não processa a fila.

## export

Gera a pasta de exportação correspondente a uma data.

```text
export --date DATA
```

`--date` é obrigatório. O comando não executa nova coleta nem aguarda jobs de mídia.

## seed-profiles

Lê `PROFILES_PATH`, cria ou atualiza o schema necessário e sincroniza os perfis com o banco.

```powershell
docker compose run --rm app python -m pipeline seed-profiles
```

O comando não coleta conteúdo. Perfis já existentes são atualizados conforme o arquivo, preservando a identidade no banco.

## init-db

Cria ou atualiza o schema embarcado usado pela aplicação:

```powershell
docker compose run --rm app python -m pipeline init-db
```

É útil em testes e instalações simples. Em ambientes mantidos, prefira `migrate`.

## migrate

Executa as migrations disponíveis de forma idempotente:

```powershell
docker compose run --rm app python -m pipeline migrate
```

Execute na primeira instalação e após atualizar o projeto. Não é necessário antes de cada coleta.

## cleanup-secondary-data

Audita ou remove dados opcionais. Sem `--confirm`, funciona obrigatoriamente como simulação.

```text
cleanup-secondary-data [--comments] [--stories]
                       [--story-media-files] [--confirm]
```

| Opção | Comportamento |
|---|---|
| `--comments` | Seleciona comentários, respostas e jobs relacionados. |
| `--stories` | Seleciona linhas e payloads de stories. |
| `--story-media-files` | Seleciona diretórios locais de mídias de stories. |
| `--confirm` | Efetiva a exclusão; sem ela, apenas mostra alvos e contagens. |

Ao menos um alvo deve ser informado. O comando não remove posts, mídias de posts, perfis ou o banco inteiro.

## Códigos de saída

| Código | Significado |
|---|---|
| `0` | Comando concluído sem falhas registradas. |
| `1` | Um ou mais perfis/jobs falharam, ou a execução ficou parcial. |
| `2` | Erro de sintaxe ou argumento rejeitado pelo `argparse`. |
| outro não zero | Exceção não tratada, interrupção ou erro do ambiente. |

Em automações, avalie o código de saída e preserve stdout/stderr em log. Uma execução que continua após erros de perfis pode terminar em `1`, mesmo tendo coletado os demais perfis corretamente.
