import json
import re
from datetime import datetime, timezone
from html import unescape
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from plugin_runtime import normalize_tag_text


def http_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    req = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=30) as response:
        return json.load(response)


def http_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as response:
        return response.read().decode("utf-8", errors="replace")


def extract_gallery_language(gid: int, token: str) -> str | None:
    html = http_text(f"https://e-hentai.org/g/{gid}/{token}/")
    match = re.search(
        r'<td class="gdt1">Language:</td><td class="gdt2">([\s\S]*?)</td>',
        html,
        re.IGNORECASE,
    )
    if not match:
        return None

    language = re.sub(r"<[^>]+>", "", match.group(1))
    language = unescape(language).strip().lower()
    return f"language:{language}" if language else None


def fetch_ehentai(gid: int, token: str) -> tuple[dict[str, Any], list[str]]:
    logs: list[str] = []
    payload = {"method": "gdata", "gidlist": [[gid, token]], "namespace": 1}
    data = http_json("https://api.e-hentai.org/api.php", payload)
    logs.append("Fetched metadata from the E-Hentai API")
    items = data.get("gmetadata") or []
    if len(items) != 1:
        raise RuntimeError(f"E-Hentai metadata not found: {gid}_{token}")

    meta = items[0]
    tags = meta.get("tags") or []
    if any(str(tag).startswith("language:") for tag in tags):
        logs.append("Language tag was present in the E-Hentai API response")
        return meta, logs

    logs.append("Language tag was missing in the API response; trying gallery page fallback")
    try:
        fallback = extract_gallery_language(gid, token)
    except (HTTPError, URLError, TimeoutError):
        fallback = None
        logs.append("Gallery page fallback request failed while resolving the language tag")

    if fallback:
        meta["tags"] = [fallback, *tags]
        logs.append(f"Gallery page fallback resolved the language tag successfully: {fallback}")
    else:
        logs.append("No language tag was found; the gallery will be updated without a language tag")

    return meta, logs


def posted_to_date(posted: Any) -> str:
    return datetime.fromtimestamp(int(posted), tz=timezone.utc).strftime("%Y-%m-%d")


def resolve_audience_tag_name(tags: list[str]) -> str:
    for tag in tags:
        if normalize_tag_text(tag).endswith("males only"):
            return "Gay"
    return "Straight"
