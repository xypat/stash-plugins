import json
import sys
from typing import Any
from urllib.error import HTTPError, URLError

from plugin_runtime import emit_info, emit_progress, load_plugin_input, read_api_key
from stash_api import (
    StashClient,
    find_galleries,
    find_gallery_by_id,
    find_performer_by_id,
    find_performers_with_rating,
    find_root_tags,
    find_scene_by_id,
    find_scenes,
    update_gallery_tags,
    update_performer_tags,
    update_scene_tags,
)

ROOT_TAG_NAMES = {
    "gallery": "__GALLERY_ATTRS__",
    "scene": "__SCENE_ATTRS__",
    "performer": "__PERFORMER_ATTRS__",
}


def resolve_graphql_url(server_connection: dict[str, Any]) -> str:
    scheme = server_connection.get("Scheme") or "http"
    host = server_connection.get("Host") or "127.0.0.1"
    if host == "0.0.0.0":
        host = "127.0.0.1"
    port = server_connection.get("Port") or 9999
    return f"{scheme}://{host}:{port}/graphql"


def normalize_name(value: str | None) -> str:
    return (value or "").strip().lower()


def find_root_tag_by_name(
    root_tags: list[dict[str, Any]], expected_name: str
) -> dict[str, Any] | None:
    normalized_expected = normalize_name(expected_name)
    for tag in root_tags:
        if normalize_name(tag.get("name")) == normalized_expected:
            return tag
        for alias in tag.get("aliases") or []:
            if normalize_name(str(alias)) == normalized_expected:
                return tag
    return None


def collect_descendant_ids(tag: dict[str, Any]) -> set[str]:
    result = {str(tag["id"])}
    for child in tag.get("children") or []:
        result.update(collect_descendant_ids(child))
    return result


def load_attr_branches(client: StashClient) -> dict[str, dict[str, Any]]:
    root_tags = find_root_tags(client)
    branches_by_type: dict[str, dict[str, Any]] = {}

    for entity_type, root_name in ROOT_TAG_NAMES.items():
        root = find_root_tag_by_name(root_tags, root_name)
        if not root:
            raise RuntimeError(f"Missing required root tag: {root_name}")

        branches: list[dict[str, Any]] = []
        attr_tag_ids: set[str] = set()
        for child in root.get("children") or []:
            descendant_ids = collect_descendant_ids(child)
            attr_tag_ids.update(descendant_ids)
            branches.append(
                {
                    "id": str(child["id"]),
                    "name": child.get("name") or "",
                    "descendant_ids": descendant_ids,
                }
            )

        if not branches:
            raise RuntimeError(f"Root tag has no attribute branches: {root_name}")

        branches_by_type[entity_type] = {
            "root_id": str(root["id"]),
            "root_name": root_name,
            "branches": branches,
            "attr_tag_ids": attr_tag_ids,
        }
        emit_info(
            f"Loaded {len(branches)} attribute branches for {entity_type} "
            f"from root id={root['id']}"
        )

    return branches_by_type


def rebuild_attr_tag_ids(
    item: dict[str, Any],
    attr_config: dict[str, Any],
) -> tuple[list[str], list[str], list[str], bool]:
    current_tag_ids = {str(tag_id) for tag_id in item.get("tag_ids") or []}
    attr_tag_ids = set(attr_config["attr_tag_ids"])
    non_attr_tag_ids = current_tag_ids - attr_tag_ids

    rebuilt_attr_tag_ids: set[str] = set()
    added_branch_ids: list[str] = []
    kept_branch_ids: list[str] = []

    for branch in attr_config["branches"]:
        branch_tag_ids = current_tag_ids & set(branch["descendant_ids"])
        if branch_tag_ids:
            specific_tag_ids = branch_tag_ids - {branch["id"]}
            if specific_tag_ids:
                rebuilt_attr_tag_ids.update(specific_tag_ids)
            else:
                rebuilt_attr_tag_ids.add(branch["id"])
            kept_branch_ids.append(branch["id"])
        else:
            rebuilt_attr_tag_ids.add(branch["id"])
            added_branch_ids.append(branch["id"])

    next_tag_ids = sorted(non_attr_tag_ids | rebuilt_attr_tag_ids, key=int)
    changed = current_tag_ids != set(next_tag_ids)
    return next_tag_ids, added_branch_ids, kept_branch_ids, changed


def should_process_item(entity_type: str, item: dict[str, Any]) -> tuple[bool, str | None]:
    if entity_type == "scene" and item.get("groups"):
        return False, "scene already has groups"

    if entity_type == "performer":
        rating100 = item.get("rating100")
        if rating100 is None or int(rating100) <= 20:
            return False, "performer rating100 is not greater than 20"

    return True, None


def apply_item_updates(
    client: StashClient,
    entity_type: str,
    items: list[dict[str, Any]],
    attr_config: dict[str, Any],
    dry_run: bool,
    progress_state: dict[str, int],
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    checked = len(items)
    updated = 0
    skipped = 0
    entity_total = len(items)
    total_items = progress_state["total"]

    if entity_total == 0:
        return {"checked": checked, "updated": 0, "skipped": 0, "results": []}

    for item in items:
        item_id = str(item["id"])
        allowed, reason = should_process_item(entity_type, item)
        if not allowed:
            skipped += 1
            emit_info(f"{entity_type} id={item_id} skipped={reason}")
            progress_state["done"] += 1
            emit_progress(progress_state["done"] / total_items)
            continue

        next_tag_ids, added_branch_ids, kept_branch_ids, changed = rebuild_attr_tag_ids(
            item, attr_config
        )
        if not changed:
            skipped += 1
            progress_state["done"] += 1
            emit_progress(progress_state["done"] / total_items)
            continue

        if not dry_run:
            if entity_type == "gallery":
                update_gallery_tags(client, item_id, next_tag_ids)
            elif entity_type == "scene":
                update_scene_tags(client, item_id, next_tag_ids)
            elif entity_type == "performer":
                update_performer_tags(client, item_id, next_tag_ids)
            else:
                raise RuntimeError(f"Unsupported entity type: {entity_type}")

        updated += 1
        results.append(
            {
                "id": item_id,
                "added_branch_ids": added_branch_ids,
                "kept_branch_ids": kept_branch_ids,
                "dry_run": dry_run,
            }
        )
        emit_info(
            f"{entity_type} id={item_id} "
            f"added_count={len(added_branch_ids)} "
            f"kept_count={len(kept_branch_ids)}"
        )
        progress_state["done"] += 1
        emit_progress(progress_state["done"] / total_items)

    return {"checked": checked, "updated": updated, "skipped": skipped, "results": results}


def select_items_for_entity(
    client: StashClient,
    entity_type: str,
    hook_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not hook_context:
        if entity_type == "gallery":
            return find_galleries(client)
        if entity_type == "scene":
            return [item for item in find_scenes(client) if not item.get("groups")]
        if entity_type == "performer":
            return find_performers_with_rating(client)
        raise RuntimeError(f"Unsupported entity type: {entity_type}")

    item_id = hook_context.get("id")
    if item_id is None:
        return []

    if entity_type == "gallery":
        item = find_gallery_by_id(client, str(item_id))
    elif entity_type == "scene":
        item = find_scene_by_id(client, str(item_id))
    elif entity_type == "performer":
        item = find_performer_by_id(client, str(item_id))
    else:
        raise RuntimeError(f"Unsupported entity type: {entity_type}")

    return [item] if item else []


def run() -> dict[str, Any]:
    plugin_input = load_plugin_input()
    server_connection = plugin_input.get("server_connection") or {}
    args = plugin_input.get("args") or {}
    hook_context = args.get("hookContext")
    entity_type = str(args.get("entity_type") or "").strip().lower()
    dry_run = str(args.get("dry_run", "false")).strip().lower() == "true"

    client = StashClient(
        graphql_url=resolve_graphql_url(server_connection),
        api_key=read_api_key(server_connection.get("Dir")),
        session_cookie=server_connection.get("SessionCookie"),
    )

    emit_info(
        f"Starting Extended Attributes dry_run={dry_run} "
        f"entity_type={entity_type or 'all'} hook={bool(hook_context)}"
    )
    attr_configs = load_attr_branches(client)

    entity_types = [entity_type] if entity_type else ["gallery", "scene", "performer"]
    output: dict[str, Any] = {"dry_run": dry_run}
    items_by_entity: dict[str, list[dict[str, Any]]] = {}

    for current_entity_type in entity_types:
        items = select_items_for_entity(client, current_entity_type, hook_context)
        items_by_entity[current_entity_type] = items
        emit_info(f"Loaded {len(items)} {current_entity_type} items")

    total_items = sum(len(items) for items in items_by_entity.values())
    progress_state = {"done": 0, "total": max(total_items, 1)}
    emit_progress(0.0)

    for current_entity_type in entity_types:
        output[current_entity_type] = apply_item_updates(
            client=client,
            entity_type=current_entity_type,
            items=items_by_entity[current_entity_type],
            attr_config=attr_configs[current_entity_type],
            dry_run=dry_run,
            progress_state=progress_state,
        )

    emit_progress(1.0)

    return {"error": None, "output": output}


def main() -> None:
    try:
        result = run()
    except (HTTPError, URLError, TimeoutError, RuntimeError, ValueError, KeyError) as exc:
        print(json.dumps({"error": str(exc), "output": None}, ensure_ascii=False))
        sys.exit(1)

    output = result.get("output") or {}
    emit_info(
        "Task finished: "
        f"gallery_updated={(output.get('gallery') or {}).get('updated', 0)} "
        f"scene_updated={(output.get('scene') or {}).get('updated', 0)} "
        f"performer_updated={(output.get('performer') or {}).get('updated', 0)}"
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
