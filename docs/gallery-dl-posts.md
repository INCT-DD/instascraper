# Coleta de posts, mídias e métricas

Este documento descreve como publicações são descobertas, normalizadas e atualizadas. Embora o nome histórico do arquivo mencione `gallery-dl`, a coleta de posts utiliza uma cadeia de backends; `gallery-dl` também permanece responsável por stories e por parte do download de mídia.

## Separação entre descoberta e download

A pipeline possui duas etapas independentes:

1. **Descoberta:** consulta a timeline, normaliza metadados, persiste posts e identifica mídias.
2. **Download:** o `media-worker` consome jobs e grava os arquivos localmente.

Um post persistido não significa que todas as suas mídias já foram baixadas. Essa separação permite continuar descobrindo posts enquanto downloads maiores ocorrem em segundo plano.

## Backends de posts

`POSTS_BACKEND` controla a estratégia.

| Valor | Comportamento |
|---|---|
| `auto` | Tenta timeline GraphQL, depois REST e, em falhas compatíveis, o adaptador baseado no `gallery-dl`. |
| `graphql` | Usa apenas a timeline GraphQL autenticada. |
| `scraper` | Usa o coletor REST do projeto e pode recorrer ao adaptador `gallery-dl` conforme a classe da falha. |
| `gallery-dl` | Usa diretamente a API REST interna exposta pelo extrator instalado. |

Falhas de autenticação, bloqueio ou contrato são classificadas para evitar que uma resposta recusada seja tratada como timeline vazia. O resultado falho é registrado por perfil e a pipeline segue para os demais.

## Timeline GraphQL

O backend principal envia `POST /graphql/query` com o `doc_id` configurado em `INSTAGRAM_TIMELINE_DOC_ID`. A resposta válida precisa conter a conexão da timeline, nodes e informações de paginação; HTTP 200 isolado não comprova sucesso.

Cada página normalmente contém vários posts e corresponde a uma requisição. A paginação continua até uma destas condições:

- não existir próxima página;
- o cursor se repetir ou estiver ausente;
- os posts regulares da página forem anteriores ao início do período;
- no modo incremental, aparecer um post regular já conhecido.

Posts fixados não definem o limite cronológico, pois podem ser antigos e aparecer no topo da primeira página.

## REST e adaptador gallery-dl

O backend REST resolve o perfil e consulta o feed autenticado. O adaptador `gallery-dl` usa a biblioteca instalada para autenticação e resolução do usuário, preservando os itens REST antes da transformação simplificada feita pelo extrator padrão.

Esse cuidado é necessário para manter:

- identificadores e shortcode;
- legenda e acessibilidade;
- contadores presentes no payload;
- variantes de imagem e vídeo;
- itens de carrossel;
- payload bruto para auditoria.

A integração depende de APIs privadas e de detalhes internos da versão instalada do `gallery-dl`. Atualizações da biblioteca devem ser acompanhadas pela suíte de compatibilidade.

## Coleta incremental

`--new-only` carrega do banco os identificadores dos posts mais recentes de cada perfil e os entrega ao backend selecionado.

O algoritmo:

1. consulta a primeira página;
2. ignora posts conhecidos em vez de enviá-los ao `upsert`;
3. preserva posts novos encontrados na mesma página;
4. não encerra por um post fixado conhecido;
5. encerra ao encontrar o primeiro post regular conhecido.

Exemplo:

```powershell
docker compose run --rm app python -m pipeline collect-posts --start-date 2026-09-01 --end-date 2026-09-15 --no-comments --new-only --rps 0.2
```

O modo reduz páginas e processamento redundantes, mas não elimina a requisição inicial de cada perfil. Se nenhum dos identificadores recentes for encontrado, a paginação continua até o limite temporal; isso privilegia completude em vez de assumir que não há posts novos.

`--new-only` não se aplica a stories. Stories são consultados somente entre os conteúdos ativos e a deduplicação ocorre na persistência e na fila.

## Normalização

Os backends convergem para o mesmo contrato interno.

| Coluna | Fontes observadas |
|---|---|
| `platform_post_id` | `pk` ou `id`. |
| `shortcode` | `code` ou `shortcode`. |
| `taken_at` | Timestamp da publicação. |
| `media_type` | Foto, vídeo ou carrossel. |
| `caption` | Texto da legenda. |
| `likes` | `like_count`. |
| `comments_count` | `comment_count`. |
| `views` | Primeiro valor disponível entre `play_count`, `ig_play_count`, `video_view_count` e `view_count`. |
| `reposts` | `media_repost_count` e variantes compatíveis de repost/share/reshare. |
| `raw_json` | Payload de descoberta acrescido de metadados do coletor. |

Ausência e zero têm significados diferentes. Campo ausente é armazenado como desconhecido; zero representa uma medição fornecida pela plataforma.

## Por que GraphQL não preenche todas as métricas

O payload atual da timeline GraphQL foi observado com `view_count` ausente ou nulo na maioria dos vídeos e sem contadores de repost. Isso não significa necessariamente que o post tenha zero visualizações ou reposts: a timeline simplesmente não forneceu o dado.

O endpoint REST de detalhe `/api/v1/media/{platform_post_id}/info/` fornece, dependendo do post e da sessão, campos como:

- `play_count`;
- `ig_play_count`;
- `media_repost_count`;
- `like_count`;
- `comment_count`.

Por isso, descoberta e atualização de métricas são casos de uso separados.

## Atualização de métricas

`refresh-post-metrics` seleciona posts já armazenados e consulta o detalhe de cada um.

```powershell
docker compose run --rm app python -m pipeline refresh-post-metrics --start-date 2026-09-01 --end-date 2026-09-15 --username lulaoficial --limit 100 --rps 0.1
```

Regras de persistência:

- valores retornados atualizam curtidas, comentários, visualizações e reposts;
- `null` ou campo ausente preserva o valor anterior;
- zero substitui o valor anterior;
- o `raw_json` original da descoberta não é substituído;
- a resposta de detalhe é gravada em `raw_payloads` como `post_metrics`;
- sessões alternativas podem ser tentadas quando a rotação está habilitada.

O custo é de aproximadamente uma requisição por post, além de eventuais tentativas de sessão. Use `--limit` e uma taxa conservadora em bases grandes.

## Identificação e download de mídias

Na descoberta, cada variante principal é transformada em um item de `post_media`. Carrosséis produzem múltiplos itens ordenados. O registro contém URL de origem, tipo, dimensões e status de download.

O worker baixa a URL direta fornecida pelo Instagram/CDN. Ele não abre a página pública do post para obter o arquivo. Dependendo do vídeo, o `gallery-dl` pode delegar formatos ao `yt-dlp` e tentar URLs alternativas.

URLs de CDN possuem assinatura e expiração. Um job processado muito tempo depois pode receber 403 mesmo com cookies válidos.

## Renovação de URLs falhadas

Para posts com mídia em `failed` ou `retry`:

```powershell
docker compose run --rm app python -m pipeline refresh-failed-media --start-date 2026-09-01 --end-date 2026-09-15 --limit 100
docker compose up -d media-worker
```

A renovação consulta novamente o post afetado, atualiza os assets e reabre os jobs aplicáveis. Ela não refaz toda a coleta do período.

## Stories

Stories são coletados pelo `gallery-dl` a partir dos itens ativos do perfil. Quando `MEDIA_QUEUE_ENABLED=true`, `collect-stories` cria jobs com prioridade superior à mídia de posts. O worker pode estar baixando posts, mas selecionará os stories pendentes prioritários nos ciclos seguintes.

O `gallery-dl` gera requisições para descobrir stories e outras para cada arquivo. `GALLERY_DL_SLEEP_REQUEST` e `GALLERY_DL_SLEEP_DOWNLOAD` controlam essas etapas separadamente.

## Limitações e interpretação

- Endpoints utilizados não são uma API acadêmica pública e podem mudar sem aviso.
- Métricas disponíveis variam por tipo de mídia, perfil, sessão e momento.
- `media_repost_count` é preservado como `reposts`, mas sua interpretação deve ser descrita metodologicamente; não se deve assumir equivalência universal com compartilhamentos externos.
- Um HTTP 200 pode conter erro lógico ou estrutura inesperada.
- Rotação de contas não garante acesso e não substitui espera após rate limit.
- Downloads bem-sucedidos dependem da validade das URLs, disponibilidade do conteúdo e espaço em disco.
- Conteúdo removido antes da descoberta não pode ser recuperado pela pipeline.

## Referências técnicas

- [Configuração do gallery-dl](https://gdl-org.github.io/docs/configuration.html)
- [Extrator Instagram do gallery-dl](https://github.com/mikf/gallery-dl/blob/master/gallery_dl/extractor/instagram.py)
- [Documentação do httpx](https://www.python-httpx.org/)
