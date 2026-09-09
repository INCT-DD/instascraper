# Posts: endpoints e preservacao dos metadados

## Timeline GraphQL como backend principal

Em 08/09/2026, a operacao de timeline usada pelo Instaloader foi validada com a sessao existente: duas paginas de 12 posts, sem IDs repetidos entre paginas. A consulta usa `POST /graphql/query`, doc_id `7898261790222653` e a variavel `__relay_internal__pv__PolarisFeedShareMenurelayprovider=false`. Apenas HTTP 200 nao comprova sucesso: a conexao `data.xdt_api__v1__feed__user_timeline_graphql_connection`, seus nodes e os dados de paginacao precisam existir.

`POSTS_BACKEND=auto` comeca por GraphQL; falhas de contrato/transporte podem usar REST e gallery-dl. `POSTS_BACKEND=graphql` seleciona apenas a timeline. `INSTAGRAM_TIMELINE_DOC_ID` permite atualizar o identificador sem alterar a persistencia. O adaptador envia CSRF, filtra as datas, elimina IDs duplicados e preserva o node bruto em `raw_json`, com a origem em `_collector`. A timeline nao retornou reposts no teste; o adaptador nao faz chamadas adicionais por post para preencher essa metrica.

Respostas 401/403 da timeline encerram somente a tentativa do perfil, registrada como failed, e a coleta segue para o proximo perfil respeitando o limitador compartilhado. Nao ha fallback nem troca de conta para repetir aquele perfil. A recusa ainda pode ser um problema da sessao inteira; a continuacao nao comprova que ela esteja valida. `collect-posts --retry-failed` repete apenas as ultimas tentativas falhadas do mesmo intervalo; `--resume` inclui tambem perfis interrompidos ou nunca tentados. Ambos usam o historico existente e nao resetam jobs de midia.

Respostas 429, redirecionamentos e feedback/challenge/checkpoint da timeline ainda interrompem a coleta sem fallback. A run do perfil termina como failed e a diaria como partial, com o motivo no relatorio; nesse caso os perfis seguintes nao sao consultados. Esta protecao vale para a execucao atual, sem cooldown persistente e sem parar workers independentes.

As verificacoes abaixo documentam o fallback REST e o GraphQL antigo do gallery-dl, distintos da timeline agora utilizada. A prioridade de stories na fila e o download HTTP das midias permanecem os mesmos.

## Verificacao anterior a implementacao

Foram consultadas as colunas reais de `posts` e `post_media` no PostgreSQL e a implementacao instalada do gallery-dl 1.32.11. Tambem foi feita uma consulta real de detalhe de um post, antes de implementar o adaptador.

O parser padrao `_parse_post_rest` seleciona somente parte do payload do Instagram. O adaptador captura os itens do extrator antes dessa transformacao. A opcao `metadata` do Instagram no gallery-dl amplia informacoes do usuario; ela nao restaura as metricas de posts descartadas pelo parser.

## Correspondencia com o banco

| Coluna | Origem |
|---|---|
| `id`, `profile_id` | Identificadores locais da pipeline |
| `platform_post_id` | `pk`, com alternativa `id` |
| `shortcode`, `url` | `code` e URL publica derivada |
| `taken_at`, `taken_at_iso` | `taken_at`, convertido para ISO em UTC |
| `media_type`, `is_video` | `media_type` REST; mantida a convencao atual para carrosseis |
| `caption` | `caption.text` |
| `likes` | `like_count`, quando disponibilizado |
| `comments_count` | `comment_count`, quando disponibilizado |
| `reposts` | Mesma lista de chaves do projeto, incluindo `media_repost_count` |
| `views` | `play_count` ou `view_count`, quando disponibilizado |
| `accessibility_caption` | Campo homonimo, quando disponibilizado |
| `raw_json` | Item REST completo, mais `_collector` com origem e versao |
| `collected_at`, `updated_at` | Gerados pelo banco |
| `post_media` | URLs, indices e dimensoes de `image_versions2`, `video_versions` e `carousel_media`; caminhos e status gerados pela pipeline |

Campos ausentes nao sao inferidos como zero. Em atualizacoes, valores anteriores das metricas sao preservados quando a resposta omite o campo; `raw_json` registra a resposta mais recente e permite identificar essa ausencia. Um valor preservado nao constitui uma nova medicao. O campo `reposts` mantem a convencao existente do projeto: as chaves de repost e compartilhamento devem ser avaliadas no payload, pois nao garantem equivalencia conceitual.

## Endpoints observados

Testes limitados em 06/09/2026, com a sessao configurada e `abmarinho`:

| Operacao | Endpoint | Resultado observado |
|---|---|---|
| Consulta inicial do scraper no log recebido | `/api/v1/users/web_profile_info/` | HTTP 429 |
| Resolucao de usuario pelo gallery-dl | `/web/search/topsearch/` | HTTP 200 |
| Listagem REST pelo gallery-dl | `/api/v1/feed/user/173847131/` | Redirecionamento para a pagina inicial |
| Detalhe de post pelo gallery-dl | `/api/v1/media/3975322793350253472/info/` | HTTP 200 |
| Listagem GraphQL pelo gallery-dl | `/graphql/query/` | HTTP 400 |
| Stories, teste anterior | `/api/v1/feed/reels_media/` no extrator REST | Uma imagem baixada e um registro persistido |

No detalhe de `DcrLwS1J8ug`, vieram 45 comentarios, 36 reposts, legenda, acessibilidade e uma imagem. Visualizacoes nao vieram nesse post de foto. O parser padrao do gallery-dl descartou comentarios, reposts e acessibilidade ao transformar a mesma resposta.

O gallery-dl usa um cookie jar, acompanha CSRF e `X-IG-WWW-Claim` e resolve usuarios por `search`/`web`. Contudo, a listagem REST usa a mesma rota de feed do scraper. O sucesso do detalhe de um post conhecido nao permite descobrir todas as publicacoes novas quando o feed esta indisponivel. O GraphQL antigo do gallery-dl, baseado em query_hash, continua desabilitado; a nova timeline com doc_id e um adaptador separado.

Os resultados descrevem uma sessao e um momento; nao demonstram bloqueio geral da conta ou indisponibilidade de todos os perfis.

## Limites operacionais

- `POSTS_BACKEND=auto` tenta GraphQL, scraper REST e gallery-dl; os demais valores selecionam somente um backend.
- O adaptador REST preserva a sessao selecionada e as datas UTC da coleta por periodo.
- A varredura ignora fixados antigos e termina apos 30 posts antigos nao fixados consecutivos. Depende da ordem cronologica do feed, assim como o coletor atual.
- Erro ou timeout antes de concluir a extracao resulta em falha, sem anunciar um subconjunto como coleta completa.
- Cookies passam por stdin do subprocesso, nunca por argumentos de linha de comando.
- A biblioteca deve estar instalada no mesmo ambiente Python; atualizacoes devem executar os testes de compatibilidade do adaptador.
- As URLs do CDN podem expirar antes do processamento. Enfileirar uma midia nao comprova seu download.
- A lista de posts conhecidos no banco nao substitui uma listagem falhada, pois isso esconderia publicacoes ainda nao descobertas.

## Fontes

- [Timeline autenticada do Instaloader](https://github.com/instaloader/instaloader/blob/master/instaloader/structures.py)
- [Paginacao por doc_id](https://github.com/instaloader/instaloader/blob/master/instaloader/nodeiterator.py)

- [Extrator na versao inspecionada](https://github.com/mikf/gallery-dl/blob/v1.32.11/gallery_dl/extractor/instagram.py)
- [Estrategias para resolver usuarios](https://gdl-org.github.io/docs/configuration.html#extractorinstagramuser-strategy)
- [Opcao metadata](https://gdl-org.github.io/docs/configuration.html#extractorinstagrammetadata)
