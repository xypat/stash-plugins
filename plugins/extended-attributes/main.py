import json
import re
import sys
from typing import Any
from urllib.error import HTTPError, URLError

from plugin_runtime import emit_info, emit_progress, emit_warn, load_plugin_input, read_api_key
from stash_api import (
    StashClient,
    bulk_update_gallery_tags,
    bulk_update_group_tags,
    bulk_update_image_tags,
    bulk_update_performer_tags,
    bulk_update_scene_tags,
    find_galleries,
    find_gallery_by_id,
    find_group_by_id,
    find_groups,
    find_image_by_id,
    find_images,
    find_performer_by_id,
    find_performers,
    find_root_tag_by_name,
    find_scene_by_id,
    find_scenes,
    find_tag_by_id,
)

ROOT_TAG_NAMES = {
    "gallery": "__GALLERY__",
    "group": "__GROUP__",
    "image": "__IMAGE__",
    "scene": "__SCENE__",
    "performer": "__PERFORMER__",
}

MULTI_SELECT_CUSTOM_FIELD = "extended_attributes_multi_select"
WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"^[a-zA-Z]:[\\/]")


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


def collect_descendant_ids(tag: dict[str, Any]) -> set[str]:
    result = {str(tag["id"])}
    for child in tag.get("children") or []:
        result.update(collect_descendant_ids(child))
    return result


def is_absolute_path_alias(value: str) -> bool:
    path = value.strip()
    return (
        path.startswith("/")
        or path.startswith("\\\\")
        or path.startswith("//")
        or bool(WINDOWS_ABSOLUTE_PATH_RE.match(path))
    )


def normalize_path(value: str) -> str:
    normalized = value.strip().replace("\\", "/")
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    if len(normalized) > 1:
        normalized = normalized.rstrip("/")
    return normalized.casefold()


def path_matches_alias(path: str, alias: str) -> bool:
    normalized_path = normalize_path(path)
    normalized_alias = normalize_path(alias)
    return normalized_path == normalized_alias or normalized_path.startswith(f"{normalized_alias}/")


def collect_tag_path_aliases(tag: dict[str, Any]) -> list[str]:
    return [
        str(alias).strip()
        for alias in tag.get("aliases") or []
        if is_absolute_path_alias(str(alias))
    ]


def build_tag_node(tag: dict[str, Any]) -> dict[str, Any]:
    children = [build_tag_node(child) for child in tag.get("children") or []]
    aliases = [str(alias).strip() for alias in tag.get("aliases") or []]
    return {
        "id": str(tag["id"]),
        "name": tag.get("name") or "",
        "aliases": aliases,
        "multi_select": bool((tag.get("custom_fields") or {}).get(MULTI_SELECT_CUSTOM_FIELD)),
        "path_aliases": collect_tag_path_aliases(tag),
        "children": children,
        "descendant_ids": collect_descendant_ids(tag),
    }


def flatten_tag_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for node in nodes:
        result.append(node)
        result.extend(flatten_tag_nodes(node["children"]))
    return result


def load_type_config(client: StashClient, entity_type: str) -> dict[str, Any] | None:
    root_name = ROOT_TAG_NAMES[entity_type]
    root = find_root_tag_by_name(client, root_name)
    if not root:
        emit_warn(f"Root tag {root_name} was not found; skipping {entity_type}")
        return None

    subtype_tags = [
        find_tag_by_id(client, str(child["id"])) for child in root.get("children") or []
    ]
    subtypes = [build_tag_node(tag) for tag in subtype_tags if tag]
    if not subtypes:
        emit_warn(f"Root tag {root_name} has no subtypes; skipping {entity_type}")
        return None

    all_nodes = flatten_tag_nodes(subtypes)
    config = {
        "root_id": str(root["id"]),
        "root_name": root_name,
        "subtypes": subtypes,
        "subtype_ids": {subtype["id"] for subtype in subtypes},
        "managed_tag_ids": {node["id"] for node in all_nodes},
        "all_nodes": all_nodes,
    }
    emit_info(f"Loaded {len(subtypes)} subtypes for {entity_type}")
    return config


def find_path_matched_tag_ids(paths: list[str], nodes: list[dict[str, Any]]) -> set[str]:
    if not paths:
        return set()

    return {
        node["id"]
        for node in nodes
        if node["path_aliases"]
        and any(path_matches_alias(path, alias) for path in paths for alias in node["path_aliases"])
    }


def find_selected_leaf_ids(selected_ids: set[str], attribute: dict[str, Any]) -> set[str]:
    specific_ids = selected_ids & (attribute["descendant_ids"] - {attribute["id"]})
    nodes_by_id = {node["id"]: node for node in flatten_tag_nodes(attribute["children"])}
    return {
        tag_id
        for tag_id in specific_ids
        if not (nodes_by_id[tag_id]["descendant_ids"] & specific_ids - {tag_id})
    }


def choose_single_value(value_ids: set[str], preferred_tag_ids: list[str] | None) -> str:
    for tag_id in reversed(preferred_tag_ids or []):
        if tag_id in value_ids:
            return tag_id
    return min(value_ids, key=int)


def rebuild_attr_tag_ids(
    item: dict[str, Any],
    type_config: dict[str, Any],
    preferred_tag_ids: list[str] | None = None,
    strict: bool = True,
) -> tuple[list[str], list[str], list[str], bool, list[str], list[dict[str, Any]]]:
    current_tag_ids = {str(tag_id) for tag_id in item.get("tag_ids") or []}
    paths = [str(path) for path in item.get("paths") or [] if path]
    path_matched_ids = find_path_matched_tag_ids(paths, type_config["all_nodes"])

    # An explicit subtype overrides directory defaults. Each root permits one subtype only.
    explicit_subtype_ids = type_config["subtype_ids"] & current_tag_ids
    path_subtype_ids = type_config["subtype_ids"] & path_matched_ids
    candidate_subtype_ids = explicit_subtype_ids or path_subtype_ids
    if not candidate_subtype_ids:
        return sorted(current_tag_ids, key=int), [], [], False, [], []

    active_subtype_id = choose_single_value(candidate_subtype_ids, preferred_tag_ids)
    active_subtype_ids = {active_subtype_id}
    next_tag_ids = set(current_tag_ids)
    next_tag_ids.update(active_subtype_ids)
    next_tag_ids.difference_update(type_config["subtype_ids"] - active_subtype_ids)
    added_branch_ids: list[str] = []
    kept_branch_ids: list[str] = []
    resolved_conflicts: list[dict[str, Any]] = []

    if len(candidate_subtype_ids) > 1:
        resolved_conflicts.append(
            {
                "kind": "subtype",
                "kept_subtype_id": active_subtype_id,
                "removed_subtype_ids": sorted(
                    candidate_subtype_ids - active_subtype_ids,
                    key=int,
                ),
            }
        )

    active_subtype = next(
        subtype for subtype in type_config["subtypes"] if subtype["id"] == active_subtype_id
    )
    active_descendant_ids = active_subtype["descendant_ids"]
    if strict:
        allowed_managed_ids = active_subtype_ids | active_descendant_ids
        disallowed_managed_ids = type_config["managed_tag_ids"] - allowed_managed_ids
        next_tag_ids.difference_update(disallowed_managed_ids)
    next_tag_ids.update(path_matched_ids & active_descendant_ids)

    for attribute in active_subtype["children"]:
        branch_ids = next_tag_ids & attribute["descendant_ids"]
        value_ids = find_selected_leaf_ids(branch_ids, attribute)
        next_tag_ids.difference_update(branch_ids)

        if value_ids and attribute["multi_select"]:
            next_tag_ids.update(value_ids)
            kept_branch_ids.append(attribute["id"])
        elif value_ids:
            kept_value_id = choose_single_value(value_ids, preferred_tag_ids)
            next_tag_ids.add(kept_value_id)
            kept_branch_ids.append(attribute["id"])
            if len(value_ids) > 1:
                resolved_conflicts.append(
                    {
                        "kind": "attribute",
                        "attribute_id": attribute["id"],
                        "kept_value_id": kept_value_id,
                        "removed_value_ids": sorted(value_ids - {kept_value_id}, key=int),
                    }
                )
        else:
            next_tag_ids.add(attribute["id"])
            if attribute["id"] not in branch_ids:
                added_branch_ids.append(attribute["id"])

    changed = current_tag_ids != next_tag_ids
    return (
        sorted(next_tag_ids, key=int),
        added_branch_ids,
        kept_branch_ids,
        changed,
        sorted(active_subtype_ids, key=int),
        resolved_conflicts,
    )


def bulk_update_tags(
    client: StashClient,
    entity_type: str,
    item_ids: list[str],
    tag_ids: list[str],
) -> None:
    if entity_type == "gallery":
        bulk_update_gallery_tags(client, item_ids, tag_ids)
    elif entity_type == "group":
        bulk_update_group_tags(client, item_ids, tag_ids)
    elif entity_type == "image":
        bulk_update_image_tags(client, item_ids, tag_ids)
    elif entity_type == "scene":
        bulk_update_scene_tags(client, item_ids, tag_ids)
    elif entity_type == "performer":
        bulk_update_performer_tags(client, item_ids, tag_ids)
    else:
        raise RuntimeError(f"Unsupported entity type: {entity_type}")


def apply_item_updates(
    client: StashClient,
    entity_type: str,
    items: list[dict[str, Any]],
    type_config: dict[str, Any],
    dry_run: bool,
    progress_state: dict[str, int],
    preferred_tag_ids: list[str] | None = None,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    plans: list[dict[str, Any]] = []
    skipped = 0
    total_items = progress_state["total"]

    for item in items:
        item_id = str(item["id"])
        (
            next_tag_ids,
            added_branch_ids,
            kept_branch_ids,
            changed,
            active_subtype_ids,
            resolved_conflicts,
        ) = rebuild_attr_tag_ids(item, type_config, preferred_tag_ids)

        if not active_subtype_ids or not changed:
            skipped += 1
            progress_state["done"] += 1
            emit_progress(progress_state["done"] / total_items)
            continue

        plans.append({"id": item_id, "tag_ids": next_tag_ids})
        results.append(
            {
                "id": item_id,
                "active_subtype_ids": active_subtype_ids,
                "added_branch_ids": added_branch_ids,
                "kept_branch_ids": kept_branch_ids,
                "resolved_conflicts": resolved_conflicts,
                "paths": item.get("paths") or [],
                "dry_run": dry_run,
            }
        )
        if dry_run:
            emit_info(
                f"{entity_type} id={item_id} "
                f"subtypes={active_subtype_ids} "
                f"added_count={len(added_branch_ids)}"
            )
        progress_state["done"] += 1
        emit_progress(progress_state["done"] / total_items)

    if dry_run:
        return {
            "checked": len(items),
            "updated": len(plans),
            "skipped": skipped,
            "results": results,
            "bulk_groups": 0,
        }

    grouped_plans: dict[tuple[str, ...], list[str]] = {}
    for plan in plans:
        grouped_plans.setdefault(tuple(plan["tag_ids"]), []).append(plan["id"])

    for tag_ids_key, item_ids in grouped_plans.items():
        bulk_update_tags(client, entity_type, item_ids, list(tag_ids_key))
        emit_info(f"{entity_type} bulk update items={len(item_ids)} tag_count={len(tag_ids_key)}")

    return {
        "checked": len(items),
        "updated": len(plans),
        "skipped": skipped,
        "results": results,
        "bulk_groups": len(grouped_plans),
    }


def select_items_for_entity(
    client: StashClient,
    entity_type: str,
    hook_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not hook_context:
        if entity_type == "gallery":
            return find_galleries(client)
        if entity_type == "group":
            return find_groups(client)
        if entity_type == "image":
            return find_images(client)
        if entity_type == "scene":
            return find_scenes(client)
        if entity_type == "performer":
            return find_performers(client)
        raise RuntimeError(f"Unsupported entity type: {entity_type}")

    item_id = hook_context.get("id")
    if item_id is None:
        return []

    if entity_type == "gallery":
        item = find_gallery_by_id(client, str(item_id))
    elif entity_type == "group":
        item = find_group_by_id(client, str(item_id))
    elif entity_type == "image":
        item = find_image_by_id(client, str(item_id))
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
    requested_entity_type = str(args.get("entity_type") or "").strip().lower()
    dry_run = str(args.get("dry_run", "false")).strip().lower() == "true"
    hook_input = (hook_context or {}).get("input") or {}
    input_fields = set((hook_context or {}).get("inputFields") or [])
    preferred_tag_ids = (
        [str(tag_id) for tag_id in hook_input.get("tag_ids") or []]
        if "tag_ids" in input_fields
        else None
    )

    if requested_entity_type and requested_entity_type not in ROOT_TAG_NAMES:
        raise ValueError(f"Unsupported entity type: {requested_entity_type}")

    client = StashClient(
        graphql_url=resolve_graphql_url(server_connection),
        api_key=read_api_key(server_connection.get("Dir")),
        session_cookie=server_connection.get("SessionCookie"),
    )

    entity_types = (
        [requested_entity_type]
        if requested_entity_type
        else ["gallery", "group", "image", "scene", "performer"]
    )
    type_configs = {
        entity_type: config
        for entity_type in entity_types
        if (config := load_type_config(client, entity_type)) is not None
    }
    output: dict[str, Any] = {"dry_run": dry_run}
    items_by_entity: dict[str, list[dict[str, Any]]] = {}

    for entity_type in entity_types:
        if entity_type not in type_configs:
            output[entity_type] = {
                "checked": 0,
                "updated": 0,
                "skipped": 0,
                "results": [],
                "reason": f"Missing {ROOT_TAG_NAMES[entity_type]} or its subtypes",
            }
            continue
        items = select_items_for_entity(client, entity_type, hook_context)
        items_by_entity[entity_type] = items
        emit_info(f"Loaded {len(items)} {entity_type} items")

    total_items = sum(len(items) for items in items_by_entity.values())
    progress_state = {"done": 0, "total": max(total_items, 1)}
    emit_progress(0.0)

    for entity_type, items in items_by_entity.items():
        output[entity_type] = apply_item_updates(
            client=client,
            entity_type=entity_type,
            items=items,
            type_config=type_configs[entity_type],
            dry_run=dry_run,
            progress_state=progress_state,
            preferred_tag_ids=preferred_tag_ids,
        )

    emit_progress(1.0)
    return {"error": None, "output": output}


def main() -> None:
    try:
        result = run()
    except (HTTPError, URLError, TimeoutError, RuntimeError, ValueError, KeyError) as exc:
        print(json.dumps({"error": str(exc), "output": None}, ensure_ascii=False))
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
