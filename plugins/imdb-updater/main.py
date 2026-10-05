import json
import re
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, NamedTuple
from urllib.error import HTTPError, URLError

from constants import MOVIE_GROUP_NAME, ROOT_GROUP_NAME, TV_GROUP_NAME
from imdb_episodes import Episode, load_episodes
from plugin_runtime import emit_info, emit_progress, emit_warn, load_plugin_input, read_api_key
from stash_api import (
    StashClient,
    create_group,
    find_groups,
    find_scenes,
    scrape_group_url,
    scrape_scene_url,
    update_scene,
)

IMDB_ID_RE = re.compile(r"\[imdbid=(tt\d+)\]", re.IGNORECASE)
SEASON_EPISODES_RE = re.compile(r"[Ss](\d{1,2})((?:[ ._-]?[Ee]\d{1,3})+)")
SEASON_X_EPISODE_RE = re.compile(r"\b(\d{1,2})x(\d{1,3})(?:-(\d{1,3}))?\b", re.IGNORECASE)
EPISODE_NUMBER_RE = re.compile(r"[Ee](\d{1,3})")

GROUP_SCRAPED_FIELDS = ("name", "date", "director", "synopsis", "front_image", "back_image")
SCENE_SCRAPED_FIELDS = {
    "title": "title",
    "code": "code",
    "details": "details",
    "director": "director",
    "date": "date",
    "image": "cover_image",
}

FAILURES = (HTTPError, URLError, TimeoutError, RuntimeError, ValueError, KeyError)


def imdb_url(imdb_id: str) -> str:
    return f"https://www.imdb.com/title/{imdb_id}/"


def resolve_graphql_url(server_connection: dict[str, Any]) -> str:
    explicit_url = str(server_connection.get("GraphQLURL") or "").strip()
    if explicit_url:
        return explicit_url
    scheme = server_connection.get("Scheme") or "http"
    host = server_connection.get("Host") or "127.0.0.1"
    if host == "0.0.0.0":
        host = "127.0.0.1"
    port = server_connection.get("Port") or 9999
    return f"{scheme}://{host}:{port}/graphql"


class SceneEntry(NamedTuple):
    scene: dict[str, Any]
    folder: str
    file_name: str
    episode_key: tuple[int, int] | None


def parse_episode(file_name: str) -> tuple[int, int] | None:
    if match := SEASON_EPISODES_RE.search(file_name):
        # 多集合并的文件（如 S01E23-E24）按最后一集处理。
        return int(match[1]), int(EPISODE_NUMBER_RE.findall(match[2])[-1])
    if match := SEASON_X_EPISODE_RE.search(file_name):
        return int(match[1]), int(match[3] or match[2])
    return None


def find_imdb_entry(scene: dict[str, Any]) -> tuple[str, SceneEntry] | None:
    for file in scene["files"]:
        *folders, file_name = re.split(r"[\\/]", file["path"])
        for folder in folders:
            if match := IMDB_ID_RE.search(folder):
                return match[1], SceneEntry(scene, folder, file_name, parse_episode(file_name))
    return None


def group_entries_by_imdb_id(scenes: list[dict[str, Any]]) -> dict[str, list[SceneEntry]]:
    entries_by_imdb_id: defaultdict[str, list[SceneEntry]] = defaultdict(list)
    for scene in scenes:
        if found := find_imdb_entry(scene):
            entries_by_imdb_id[found[0]].append(found[1])
    return entries_by_imdb_id


def fallback_group_name(folder: str) -> str:
    name = re.sub(r"\s*\[[^\]]*\]", "", folder)
    return re.sub(r"\s*\(\d{4}\)$", "", name).strip()


def ensure_named_group(
    client: StashClient, name: str, parent_id: str | None, dry_run: bool
) -> str | None:
    expected_parent_ids = [parent_id] if parent_id else []
    for group in find_groups(client, {"name": {"value": name, "modifier": "EQUALS"}}):
        if [item["group"]["id"] for item in group["containing_groups"]] == expected_parent_ids:
            return group["id"]

    if dry_run:
        emit_info(f"Would create group: {name}")
        return None
    containing_groups = [{"group_id": parent_id}] if parent_id else None
    created = create_group(client, {"name": name, "containing_groups": containing_groups})
    emit_info(f"Created group: {name}")
    return created["id"]


def ensure_entry_group(
    client: StashClient, imdb_id: str, folder: str, is_tv: bool, dry_run: bool
) -> str | None:
    url = imdb_url(imdb_id)
    existing = find_groups(client, {"url": {"value": url, "modifier": "EQUALS"}})
    if existing:
        return existing[0]["id"]

    root_id = ensure_named_group(client, ROOT_GROUP_NAME, None, dry_run)
    category_id = ensure_named_group(
        client, TV_GROUP_NAME if is_tv else MOVIE_GROUP_NAME, root_id, dry_run
    )
    if dry_run:
        emit_info(f"Would create entry group from {url}")
        return None

    scraped: dict[str, Any] = {}
    try:
        scraped = scrape_group_url(client, url) or {}
    except FAILURES as exc:
        emit_warn(f"Group scrape failed, creating it from the folder name: {url} ({exc})")

    group_input = {field: scraped[field] for field in GROUP_SCRAPED_FIELDS if scraped.get(field)}
    group_input.setdefault("name", fallback_group_name(folder))
    group_input["urls"] = scraped.get("urls") or [url]
    group_input["containing_groups"] = [{"group_id": category_id}]
    created = create_group(client, group_input)
    emit_info(f"Created group: {created['name']} ({url})")
    return created["id"]


def build_scene_update(
    client: StashClient,
    scene: dict[str, Any],
    group_id: str,
    scene_index: int | None,
    episode: Episode | None,
    dry_run: bool,
) -> dict[str, Any]:
    update: dict[str, Any] = {"id": scene["id"]}

    current_indexes = {item["group"]["id"]: item["scene_index"] for item in scene["groups"]}
    if group_id not in current_indexes or current_indexes[group_id] != scene_index:
        other_groups = [
            {"group_id": group_id_, "scene_index": index}
            for group_id_, index in current_indexes.items()
            if group_id_ != group_id
        ]
        update["groups"] = [*other_groups, {"group_id": group_id, "scene_index": scene_index}]

    if episode:
        url = imdb_url(episode.imdb_id)
        urls = [url, *scene["urls"]]
        if not scene["title"] and not dry_run:
            scraped = scrape_scene_url(client, url) or {}
            update.update(
                {
                    target: scraped[source]
                    for source, target in SCENE_SCRAPED_FIELDS.items()
                    if scraped.get(source)
                }
            )
            urls = [url, *(scraped.get("urls") or []), *scene["urls"]]
        unique_urls = list(dict.fromkeys(urls))
        if unique_urls != scene["urls"]:
            update["urls"] = unique_urls

    return update


def sync_series(
    client: StashClient,
    imdb_id: str,
    entries: list[SceneEntry],
    episodes: list[Episode],
    dry_run: bool,
    progress: dict[str, int],
) -> dict[str, Any]:
    is_tv = any(entry.episode_key for entry in entries)
    group_id = ensure_entry_group(client, imdb_id, entries[0].folder, is_tv, dry_run)

    episode_by_key = {(item.season, item.episode): item for item in episodes}
    # scene_index 只按库里已有的季累计，缺失的季不占位。
    owned_seasons = {entry.episode_key[0] for entry in entries if entry.episode_key}
    index_by_key = {
        (item.season, item.episode): index
        for index, item in enumerate(
            (item for item in episodes if item.season in owned_seasons and item.episode > 0),
            start=1,
        )
    }
    result: dict[str, Any] = {"updated": 0, "skipped": 0, "failed": []}

    for scene, _, file_name, key in entries:
        progress["done"] += 1
        emit_progress(progress["done"] / progress["total"])
        if group_id is None:
            result["skipped"] += 1
            continue

        if key and key not in episode_by_key:
            emit_warn(f"Episode {key} of {imdb_id} was not found in the IMDb dataset: {file_name}")
        try:
            update = build_scene_update(
                client,
                scene,
                group_id,
                index_by_key.get(key) if key else None,
                episode_by_key.get(key) if key else None,
                dry_run,
            )
            if len(update) == 1:
                result["skipped"] += 1
                continue
            if not dry_run:
                update_scene(client, update)
            result["updated"] += 1
            emit_info(
                f"{'Would update' if dry_run else 'Updated'} scene {scene['id']}: {file_name}"
            )
        except FAILURES as exc:
            result["failed"].append({"id": scene["id"], "path": file_name, "reason": str(exc)})
            emit_warn(f"Failed scene {scene['id']}: {file_name} ({exc})")

    return result


def run() -> dict[str, Any]:
    plugin_input = load_plugin_input()
    server_connection = plugin_input.get("server_connection") or {}
    args = plugin_input.get("args") or {}
    hook_context = args.get("hookContext")
    dry_run = str(args.get("dry_run", "false")).strip().lower() == "true"

    client = StashClient(
        graphql_url=resolve_graphql_url(server_connection),
        api_key=read_api_key(server_connection.get("Dir")),
        session_cookie=server_connection.get("SessionCookie"),
    )

    scene_ids = [str(hook_context["id"])] if hook_context and "id" in hook_context else None
    entries_by_imdb_id = group_entries_by_imdb_id(find_scenes(client, scene_ids))
    if scene_ids and entries_by_imdb_id:
        # 新增场景可能改变同一作品其他场景的 scene_index，所以连同整部作品一起处理。
        entries_by_imdb_id = group_entries_by_imdb_id(
            find_scenes(client, imdb_ids=list(entries_by_imdb_id))
        )
    if not entries_by_imdb_id:
        return {"error": None, "output": {"dry_run": dry_run, "series": {}}}

    tv_series_ids = {
        imdb_id
        for imdb_id, entries in entries_by_imdb_id.items()
        if any(entry.episode_key for entry in entries)
    }
    cache_path = Path(server_connection.get("Dir") or tempfile.gettempdir()) / "cache"
    episodes = load_episodes(cache_path / "imdb-updater-episodes.json", tv_series_ids)

    total = sum(len(entries) for entries in entries_by_imdb_id.values())
    progress = {"done": 0, "total": total}
    emit_info(f"Processing {total} scenes of {len(entries_by_imdb_id)} IMDb entries")
    output = {
        imdb_id: sync_series(client, imdb_id, entries, episodes.get(imdb_id, []), dry_run, progress)
        for imdb_id, entries in entries_by_imdb_id.items()
    }
    return {"error": None, "output": {"dry_run": dry_run, "series": output}}


def main() -> None:
    try:
        result = run()
    except FAILURES as exc:
        print(json.dumps({"error": str(exc), "output": None}, ensure_ascii=False))
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
