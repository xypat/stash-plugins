import gzip
import io
import json
import time
from pathlib import Path
from typing import Any, NamedTuple

# IMDb 官方的非商业数据集，包含每一集的 tt ID、所属剧集、季号和集号。
DATASET_URL = "https://datasets.imdbws.com/title.episode.tsv.gz"
CACHE_TTL_SECONDS = 24 * 60 * 60


class Episode(NamedTuple):
    season: int
    episode: int
    imdb_id: str


def read_cache(cache_path: Path) -> dict[str, Any]:
    try:
        cache = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return cache if isinstance(cache, dict) else {}


def download_episodes(series_ids: set[str]) -> dict[str, list[Episode]]:
    from urllib.request import Request, urlopen

    episodes: dict[str, list[Episode]] = {series_id: [] for series_id in series_ids}
    request = Request(DATASET_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=120) as response, gzip.GzipFile(fileobj=response) as raw:
        lines = io.TextIOWrapper(raw, encoding="utf-8")
        next(lines)  # 表头：tconst parentTconst seasonNumber episodeNumber
        for line in lines:
            episode_id, series_id, season, episode = line.rstrip("\n").split("\t")
            if series_id in series_ids and season.isdigit() and episode.isdigit():
                episodes[series_id].append(Episode(int(season), int(episode), episode_id))

    for series_episodes in episodes.values():
        series_episodes.sort()
    return episodes


def load_episodes(cache_path: Path, series_ids: set[str]) -> dict[str, list[Episode]]:
    cache = read_cache(cache_path)
    now = time.time()
    expired_ids = {
        series_id
        for series_id in series_ids
        if now - cache.get(series_id, {}).get("fetched", 0) > CACHE_TTL_SECONDS
    }
    if expired_ids:
        for series_id, episodes in download_episodes(expired_ids).items():
            cache[series_id] = {"fetched": now, "episodes": episodes}
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(cache), encoding="utf-8")

    return {
        series_id: [Episode(*item) for item in cache[series_id]["episodes"]]
        for series_id in series_ids
    }
