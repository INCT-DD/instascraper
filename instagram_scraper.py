# -*- coding: utf-8 -*-
"""
Instagram Profile Scraper — Pesquisa Acadêmica
===============================================
Raspa posts de um perfil público dentro de um intervalo de datas,
extraindo metadados, métricas, legendas, comentários e replies.

Requisitos:
    pip install httpx[http2] tqdm

Autenticação:
    Copie os cookies da sua sessão do Instagram e salve em cookies/collector-01.json
    (instruções abaixo na função load_cookies_from_json)
"""

import asyncio
import httpx
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from math import ceil
from typing import Any, Dict, List, Optional, Tuple
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

POSTS_PER_PAGE      = 12
COMMENTS_PER_PAGE   = 50
REPLIES_PER_PAGE    = 20

GRAPHQL_URL = "https://www.instagram.com/graphql/query/"

# query_hash para cada tipo de dado
HASH_PROFILE_POSTS  = "69cba40317214236af40e7efa697781d"  # posts do perfil
HASH_COMMENTS       = "97b41c52301f77ce508f55e66d17620e"  # comentários do post
HASH_REPLIES        = "70c4e529100ca5f4f96532cef7b47b13"  # replies de um comentário

COOKIE_JSON_PATH = "cookies/collector-01.json"


# ---------------------------------------------------------------------------
# Erros
# ---------------------------------------------------------------------------

class ScrapeError(Exception):
    pass

class AuthError(ScrapeError):
    pass


# ---------------------------------------------------------------------------
# Cookies
# ---------------------------------------------------------------------------

def load_cookies(cookie_json_path: str = COOKIE_JSON_PATH) -> Dict[str, str]:
    """
    Lê o arquivo JSON com sua sessão do Instagram.

    Como obter os cookies:
    1. Faça login no Instagram no Firefox ou Chrome
    2. F12 → Application (Chrome) ou Storage (Firefox) → Cookies → instagram.com
    3. Copie os valores de: sessionid, csrftoken, mid, ds_user_id
    4. Salve em cookies/collector-01.json:

    {
        "sessionid": "...",
        "csrftoken": "...",
        "mid": "...",
        "ds_user_id": "..."
    }
    """
    if not os.path.exists(cookie_json_path):
        raise AuthError(
            f"Arquivo {cookie_json_path} não encontrado.\n"
            "Crie-o com seus cookies do Instagram (veja instruções no topo do script)."
        )
    with open(cookie_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Suporta tanto formato plano quanto formato com chave "cookies"
    cookies = data.get("cookies", data)
    required = ("sessionid", "csrftoken", "mid", "ds_user_id")
    missing = [k for k in required if not cookies.get(k)]
    if missing:
        raise AuthError(f"Campos ausentes no arquivo de cookies: {missing}")
    return cookies


def build_cookie_string(cookies: Dict[str, str]) -> str:
    return (
        f"sessionid={cookies['sessionid']}; "
        f"ds_user_id={cookies['ds_user_id']}; "
        f"csrftoken={cookies['csrftoken']}; "
        f"mid={cookies['mid']}"
    )


def build_headers(referer: str, cookie_str: str) -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
        "X-IG-App-ID": "936619743392459",
        "X-CSRFToken": "",
        "Referer": referer,
        "Cookie": cookie_str,
    }


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    def __init__(self, rps: float):
        self.interval = 1.0 / max(0.1, rps)
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self):
        async with self._lock:
            delta = time.perf_counter() - self._last
            if delta < self.interval:
                await asyncio.sleep(self.interval - delta)
            self._last = time.perf_counter()


# ---------------------------------------------------------------------------
# GraphQL helper
# ---------------------------------------------------------------------------

async def graphql_get(
    client: httpx.AsyncClient,
    query_hash: str,
    variables: Dict[str, Any],
    referer: str,
    cookie_str: str,
) -> Dict[str, Any]:
    var_str = json.dumps(variables, separators=(",", ":"))
    params = {"query_hash": query_hash, "variables": var_str}
    headers = build_headers(referer, cookie_str)

    for attempt in range(3):
        try:
            r = await client.get(
                GRAPHQL_URL, params=params, headers=headers,
                timeout=20, follow_redirects=False
            )
        except httpx.RequestError as e:
            if attempt == 2:
                raise ScrapeError(f"Erro de rede: {e}")
            await asyncio.sleep(2)
            continue

        if r.status_code in (301, 302, 303, 307, 308):
            raise AuthError("Redirecionado — sessão expirada. Atualize o arquivo de cookies.")
        if r.status_code == 401:
            raise AuthError("Não autorizado (401) — atualize o arquivo de cookies.")
        if r.status_code == 429:
            wait = int(r.headers.get("retry-after", 60))
            tqdm.write(f"  Rate limit (429). Aguardando {wait}s...")
            await asyncio.sleep(wait)
            continue
        if r.status_code != 200:
            raise ScrapeError(f"HTTP {r.status_code}: {r.text[:200]}")

        try:
            return r.json()
        except Exception:
            raise ScrapeError("Resposta não é JSON válido.")

    raise ScrapeError("Falha após 3 tentativas.")


# ---------------------------------------------------------------------------
# Buscar posts do perfil
# ---------------------------------------------------------------------------

async def fetch_user_id(
    client: httpx.AsyncClient,
    username: str,
    cookie_str: str,
) -> str:
    """Obtém o user_id numérico a partir do username."""
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
    headers = build_headers(f"https://www.instagram.com/{username}/", cookie_str)
    r = await client.get(url, headers=headers, timeout=15, follow_redirects=False)
    _raise_for_instagram_redirect(r, "web_profile_info")
    if r.status_code == 200:
        data = r.json()
        try:
            return data["data"]["user"]["id"]
        except (KeyError, TypeError):
            pass

    return await fetch_user_id_from_search(client, username, cookie_str, r.status_code)


async def fetch_user_id_from_search(
    client: httpx.AsyncClient,
    username: str,
    cookie_str: str,
    previous_status: Optional[int] = None,
) -> str:
    """
    Fallback para perfis em que /web_profile_info/ quebra por campos de categoria
    profissional removidos pelo Instagram.
    """
    url = "https://www.instagram.com/web/search/topsearch/"
    headers = build_headers("https://www.instagram.com/", cookie_str)
    r = await client.get(
        url,
        params={"query": username},
        headers=headers,
        timeout=15,
        follow_redirects=False,
    )
    _raise_for_instagram_redirect(r, "topsearch")
    if r.status_code != 200:
        detail = f"HTTP {previous_status}" if previous_status else "estrutura inesperada"
        raise ScrapeError(
            f"Não foi possível obter user_id de @{username} ({detail}; fallback topsearch HTTP {r.status_code})."
        )

    try:
        data = r.json()
        wanted = username.lower()
        for result in data.get("users", []):
            user = result.get("user", {})
            if user.get("username", "").lower() == wanted:
                user_id = user.get("pk") or user.get("id")
                if user_id:
                    return str(user_id)
    except (KeyError, TypeError, ValueError):
        pass

    detail = f"HTTP {previous_status}" if previous_status else "estrutura inesperada"
    raise ScrapeError(f"Não foi possível obter user_id de @{username} ({detail}; fallback topsearch sem match).")


async def fetch_posts_page(
    client: httpx.AsyncClient,
    user_id: str,
    cookie_str: str,
    username: str,
    after: Optional[str] = None,
) -> Tuple[List[Dict], bool, Optional[str]]:
    """
    Usa o endpoint /api/v1/feed/user/{user_id}/ que é mais estável
    do que o GraphQL com query_hash (que o Instagram troca com frequência).
    """
    url = f"https://www.instagram.com/api/v1/feed/user/{user_id}/"
    params: Dict[str, Any] = {"count": POSTS_PER_PAGE}
    if after:
        params["max_id"] = after

    headers = build_headers(f"https://www.instagram.com/{username}/", cookie_str)
    # Endpoint v1 precisa deste header adicional
    headers["X-IG-App-ID"] = "936619743392459"

    data = None
    for attempt in range(3):
        try:
            r = await client.get(url, params=params, headers=headers, timeout=20, follow_redirects=False)
        except httpx.RequestError as e:
            if attempt == 2:
                raise ScrapeError(f"Erro de rede: {e}")
            await asyncio.sleep(2)
            continue

        _raise_for_instagram_redirect(r, "feed/user")
        if r.status_code == 401:
            raise AuthError("Não autorizado (401) — atualize o arquivo de cookies.")
        if r.status_code == 403:
            raise AuthError("Acesso negado (403) — cookies inválidos ou expirados.")
        if r.status_code == 429:
            wait = int(r.headers.get("retry-after", 60))
            tqdm.write(f"  Rate limit (429). Aguardando {wait}s...")
            await asyncio.sleep(wait)
            continue
        if r.status_code != 200:
            raise ScrapeError(f"HTTP {r.status_code} ao buscar posts: {r.text[:300]}")

        # Verifica se foi redirecionado para login
        if "login" in str(r.url):
            raise AuthError("Redirecionado para login — cookies inválidos ou expirados.")

        content_type = r.headers.get("content-type", "")
        if "text/html" in content_type or r.text.lstrip().lower().startswith("<!doctype html"):
            raise AuthError(
                "Instagram retornou HTML no endpoint de feed; sessao provavelmente expirada, "
                "em challenge ou sem permissao para a API. Atualize/verifique os cookies."
            )

        try:
            data = r.json()
        except Exception:
            raise ScrapeError(f"Resposta não é JSON válido. Conteúdo: {r.text[:200]}")

        break

    if data is None:
        raise ScrapeError("Nenhuma resposta válida após 3 tentativas.")

    items = data.get("items", [])
    more_available = data.get("more_available", False)
    next_max_id = data.get("next_max_id")

    # Converte formato API v1 para o formato esperado pelo parse_post_metadata
    posts = []
    for item in items:
        posts.append(_normalize_v1_item(item))

    return posts, more_available, next_max_id


def _raise_for_instagram_redirect(response: httpx.Response, context: str) -> None:
    if response.status_code not in (301, 302, 303, 307, 308):
        return

    location = response.headers.get("location", "")
    location_lower = location.lower()
    if "challenge" in location_lower:
        raise AuthError(
            f"Instagram redirecionou {context} para challenge; resolva a verificacao da conta "
            "e atualize os cookies."
        )
    if "login" in location_lower or location in {"", "/"} or location.startswith("https://www.instagram.com/"):
        raise AuthError(
            f"Instagram redirecionou {context} para {location or 'outra pagina'}; "
            "cookies invalidos, expirados ou conta em verificacao."
        )
    raise AuthError(f"Instagram redirecionou {context} para {location}; atualize/verifique os cookies.")


def _normalize_v1_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza um item da API v1 para o mesmo formato do GraphQL."""
    # Caption
    caption_text = ""
    cap = item.get("caption")
    if isinstance(cap, dict):
        caption_text = cap.get("text", "")
    elif isinstance(cap, str):
        caption_text = cap

    # Likes
    likes = item.get("like_count", 0)

    # Comments
    comments_count = item.get("comment_count", 0)

    # Views
    views = item.get("play_count") or item.get("view_count")

    # Instagram may omit this metric depending on media type/account/session.
    clips_metadata = item.get("clips_metadata") if isinstance(item.get("clips_metadata"), dict) else {}
    reposts = _first_int(
        item.get("media_repost_count"),
        item.get("media_reposts_count"),
        item.get("share_count"),
        item.get("shares_count"),
        item.get("reshare_count"),
        item.get("reshares_count"),
        item.get("repost_count"),
        item.get("reposts_count"),
        item.get("ig_repost_count"),
        item.get("ig_reposts_count"),
        clips_metadata.get("share_count"),
        clips_metadata.get("shares_count"),
        clips_metadata.get("reshare_count"),
        clips_metadata.get("reshares_count"),
        clips_metadata.get("repost_count"),
        clips_metadata.get("reposts_count"),
        clips_metadata.get("media_repost_count"),
        clips_metadata.get("media_reposts_count"),
        clips_metadata.get("ig_repost_count"),
        clips_metadata.get("ig_reposts_count"),
    )

    # Media type
    media_type_num = item.get("media_type", 1)

    return {
        "shortcode": item.get("code", ""),
        "id": item.get("pk", "") or item.get("id", ""),
        "taken_at_timestamp": item.get("taken_at", 0),
        "media_type": media_type_num,
        "__typename": {1: "GraphImage", 2: "GraphVideo", 8: "GraphSidecar"}.get(media_type_num, "GraphImage"),
        "edge_media_to_caption": {"edges": [{"node": {"text": caption_text}}]} if caption_text else {"edges": []},
        "edge_liked_by": {"count": likes},
        "edge_media_to_comment": {"count": comments_count},
        "edge_media_to_repost": {"count": reposts},
        "reposts": reposts,
        "video_view_count": views,
        "is_video": media_type_num == 2,
        "accessibility_caption": item.get("accessibility_caption"),
        "raw_json": item,
    }


def _first_int(*values: Any) -> Optional[int]:
    for value in values:
        if isinstance(value, bool) or value is None:
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            normalized = value.strip().replace(",", "").replace(".", "")
            if normalized.isdigit():
                return int(normalized)
    return None


def parse_post_metadata(node: Dict[str, Any]) -> Dict[str, Any]:
    """Extrai campos relevantes de um nó de post."""
    shortcode = node.get("shortcode", "")
    taken_at = node.get("taken_at") or node.get("taken_at_timestamp", 0)
    dt = datetime.fromtimestamp(taken_at, tz=timezone.utc).isoformat() if taken_at else None

    caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
    caption = caption_edges[0]["node"]["text"] if caption_edges else ""

    media_type_map = {1: "photo", 2: "video", 8: "carousel"}
    media_type = media_type_map.get(node.get("media_type", 1), "unknown")
    if node.get("__typename") == "GraphVideo":
        media_type = "video"
    elif node.get("__typename") == "GraphSidecar":
        media_type = "carousel"

    reposts = _first_int(
        node.get("reposts"),
        node.get("media_repost_count"),
        node.get("media_reposts_count"),
        node.get("edge_media_to_repost", {}).get("count"),
        node.get("share_count"),
        node.get("shares_count"),
        node.get("reshare_count"),
        node.get("reshares_count"),
        node.get("repost_count"),
        node.get("reposts_count"),
        node.get("ig_repost_count"),
        node.get("ig_reposts_count"),
    )

    return {
        "shortcode": shortcode,
        "post_id": node.get("id", ""),
        "url": f"https://www.instagram.com/p/{shortcode}/",
        "taken_at": taken_at,
        "taken_at_iso": dt,
        "media_type": media_type,
        "caption": caption,
        "likes": node.get("edge_liked_by", {}).get("count", 0),
        "comments_count": node.get("edge_media_to_comment", {}).get("count", 0),
        "reposts": reposts,
        "views": node.get("video_view_count"),
        "is_video": node.get("is_video", False),
        "accessibility_caption": node.get("accessibility_caption"),
        "raw_json": node.get("raw_json", node),
        "comments": [],
    }


# ---------------------------------------------------------------------------
# Buscar comentários
# ---------------------------------------------------------------------------

async def fetch_comments_for_post(
    client: httpx.AsyncClient,
    shortcode: str,
    cookie_str: str,
    limiter: RateLimiter,
    fetch_replies: bool = True,
    max_comments: Optional[int] = None,
) -> List[Dict[str, Any]]:
    referer = f"https://www.instagram.com/p/{shortcode}/"
    all_comments: List[Dict[str, Any]] = []

    # --- Paginação dos comentários principais ---
    after = None
    while True:
        await limiter.wait()
        variables: Dict[str, Any] = {"shortcode": shortcode, "first": COMMENTS_PER_PAGE}
        if after:
            variables["after"] = after

        data = await graphql_get(client, HASH_COMMENTS, variables, referer, cookie_str)

        try:
            edge_info = data["data"]["shortcode_media"]["edge_media_to_parent_comment"]
            edges = edge_info["edges"]
            page_info = edge_info["page_info"]
        except (KeyError, TypeError):
            break

        for edge in edges:
            node = edge.get("node", {})
            comment = parse_comment_node(node)

            # --- Replies deste comentário ---
            if fetch_replies:
                reply_edges = node.get("edge_threaded_comments", {}).get("edges", [])
                for re_ in reply_edges:
                    comment["replies"].append(parse_comment_node(re_.get("node", {})))

                # Paginar replies se houver mais
                reply_page = node.get("edge_threaded_comments", {}).get("page_info", {})
                if reply_page.get("has_next_page") and reply_page.get("end_cursor"):
                    extra_replies = await fetch_replies_for_comment(
                        client, node.get("id", ""), reply_page["end_cursor"],
                        cookie_str, referer, limiter
                    )
                    comment["replies"].extend(extra_replies)

            all_comments.append(comment)
            if max_comments is not None and len(all_comments) >= max_comments:
                return all_comments

        if not page_info.get("has_next_page") or not page_info.get("end_cursor"):
            break
        after = page_info["end_cursor"]

    return all_comments


def parse_comment_node(node: Dict[str, Any]) -> Dict[str, Any]:
    created_at = node.get("created_at", 0)
    dt = datetime.fromtimestamp(created_at, tz=timezone.utc).isoformat() if created_at else None
    return {
        "comment_id": node.get("id", ""),
        "username": node.get("owner", {}).get("username", ""),
        "user_id": node.get("owner", {}).get("id", ""),
        "text": node.get("text", ""),
        "created_at": created_at,
        "created_at_iso": dt,
        "likes": node.get("edge_liked_by", {}).get("count", 0),
        "raw_json": node,
        "replies": [],
    }


async def fetch_replies_for_comment(
    client: httpx.AsyncClient,
    comment_id: str,
    first_cursor: str,
    cookie_str: str,
    referer: str,
    limiter: RateLimiter,
) -> List[Dict[str, Any]]:
    replies = []
    after = first_cursor
    while after:
        await limiter.wait()
        variables = {"comment_id": comment_id, "first": REPLIES_PER_PAGE, "after": after}
        try:
            data = await graphql_get(client, HASH_REPLIES, variables, referer, cookie_str)
            edge_info = data["data"]["comment"]["edge_threaded_comments"]
            for edge in edge_info.get("edges", []):
                replies.append(parse_comment_node(edge.get("node", {})))
            page_info = edge_info.get("page_info", {})
            after = page_info.get("end_cursor") if page_info.get("has_next_page") else None
        except (ScrapeError, KeyError, TypeError):
            break
    return replies


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

async def scrape_profile(
    username: str,
    date_from: datetime,
    date_to: datetime,
    cookie_str: str,
    rps: float,
) -> List[Dict[str, Any]]:

    limiter = RateLimiter(rps)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)

    async with httpx.AsyncClient(
        http2=True,
        timeout=httpx.Timeout(25.0, connect=10.0),
        limits=limits,
    ) as client:

        # 1. Obtém user_id
        print(f"\nBuscando user_id de @{username}...")
        user_id = await fetch_user_id(client, username, cookie_str)
        print(f"  → user_id: {user_id}")

        # 2. Coleta posts no intervalo de datas
        print(f"\nColetando posts entre {date_from.date()} e {date_to.date()}...")
        collected_posts: List[Dict[str, Any]] = []
        stop = False
        after = None
        page = 0

        ts_from = int(date_from.timestamp())
        ts_to   = int(date_to.timestamp())

        with tqdm(desc="Buscando posts", unit="post") as bar:
            while not stop:
                await limiter.wait()
                posts_raw, has_next, after = await fetch_posts_page(
                    client, user_id, cookie_str, username, after
                )

                page_timestamps = [
                    node.get("taken_at") or node.get("taken_at_timestamp", 0)
                    for node in posts_raw
                ]

                for node in posts_raw:
                    # API v1 usa "taken_at", GraphQL usava "taken_at_timestamp"
                    taken_at = node.get("taken_at") or node.get("taken_at_timestamp", 0)

                    if taken_at > ts_to:
                        continue  # post mais recente que o intervalo, pula
                    if taken_at < ts_from:
                        continue  # posts fixados/fora de ordem podem aparecer antes dos recentes

                    meta = parse_post_metadata(node)
                    collected_posts.append(meta)
                    bar.update(1)

                if page_timestamps and max(page_timestamps) < ts_from:
                    stop = True  # a página inteira já está mais antiga que o intervalo

                if not has_next or not after:
                    break
                page += 1

        print(f"\n  → {len(collected_posts)} posts encontrados no intervalo.")

        if not collected_posts:
            return []

        # 3. Busca comentários e replies para cada post
        print("\nBuscando comentários e replies...")
        for i, post in enumerate(tqdm(collected_posts, desc="Posts", unit="post")):
            shortcode = post["shortcode"]
            n_comments = post["comments_count"]
            tqdm.write(f"  [{i+1}/{len(collected_posts)}] /p/{shortcode}/ — {n_comments} comentários")

            if n_comments == 0:
                continue

            try:
                comments = await fetch_comments_for_post(
                    client, shortcode, cookie_str, limiter, fetch_replies=True
                )
                post["comments"] = comments
            except ScrapeError as e:
                tqdm.write(f"    ⚠ Erro ao buscar comentários: {e}")

        return collected_posts


# ---------------------------------------------------------------------------
# Saída
# ---------------------------------------------------------------------------

def save_json(posts: List[Dict], username: str, date_from: datetime, date_to: datetime):
    os.makedirs("output", exist_ok=True)
    fname = (
        f"output/{username}_"
        f"{date_from.strftime('%Y%m%d')}_"
        f"{date_to.strftime('%Y%m%d')}.json"
    )
    total_comments = sum(len(p["comments"]) for p in posts)
    total_replies  = sum(
        len(c["replies"]) for p in posts for c in p["comments"]
    )
    payload = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "username": username,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "total_posts": len(posts),
        "total_comments": total_comments,
        "total_replies": total_replies,
        "posts": posts,
    }
    tmp = fname + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, fname)
    print(f"\n✓ Salvo em: {fname}")
    print(f"  Posts: {len(posts)} | Comentários: {total_comments} | Replies: {total_replies}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_date(s: str) -> datetime:
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Data inválida: '{s}'. Use DD/MM/AAAA ou AAAA-MM-DD.")


def prompt_rps() -> float:
    while True:
        val = input("Requisições por segundo (recomendado: 1 ou 2): ").strip() or "1"
        try:
            rps = float(val)
            if rps > 0:
                return rps
            print("Digite um valor positivo.")
        except ValueError:
            print("Valor inválido.")


async def amain():
    print("=" * 55)
    print("  Instagram Profile Scraper — Pesquisa Acadêmica")
    print("=" * 55)

    username = input("\nUsername do perfil (sem @): ").strip().lstrip("@")
    if not username:
        print("Username inválido.")
        sys.exit(1)

    while True:
        try:
            date_from = parse_date(input("Data inicial (DD/MM/AAAA): ").strip())
            date_to   = parse_date(input("Data final   (DD/MM/AAAA): ").strip())
            # date_to: inclui o dia inteiro
            date_to = date_to.replace(hour=23, minute=59, second=59)
            if date_from > date_to:
                print("Data inicial deve ser anterior à data final.")
                continue
            break
        except ValueError as e:
            print(e)

    rps = prompt_rps()

    try:
        cookies = load_cookies()
    except AuthError as e:
        print(f"\n✗ {e}")
        sys.exit(1)

    cookie_str = build_cookie_string(cookies)

    try:
        posts = await scrape_profile(username, date_from, date_to, cookie_str, rps)
    except AuthError as e:
        print(f"\n✗ Autenticação: {e}")
        sys.exit(1)
    except ScrapeError as e:
        print(f"\n✗ Erro: {e}")
        sys.exit(1)

    if not posts:
        print("\nNenhum post encontrado no intervalo informado.")
        sys.exit(0)

    save_json(posts, username, date_from, date_to)


def main():
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"\nErro inesperado: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
