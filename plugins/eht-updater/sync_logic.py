from typing import Any
from urllib.error import HTTPError, URLError

from constants import (
    AUDIENCE_ROOT_TAG_NAME,
    AUDIENCE_TAG_NAME_GAY,
    AUDIENCE_TAG_NAME_STRAIGHT,
    LANGUAGE_ROOT_TAG_NAME,
)
from ehentai_api import fetch_ehentai, posted_to_date, resolve_audience_tag_name
from plugin_runtime import (
    bool_arg,
    emit_info,
    emit_progress,
    emit_warn,
    normalize_tag_text,
    parse_gallery_dir,
    to_title_case,
)
from stash_api import (
    StashClient,
    create_tag,
    find_galleries,
    find_gallery_by_id,
    search_tags,
    update_gallery,
)


def matches_tag_name(tag: dict[str, Any], expected_name: str) -> bool:
    normalized_expected = normalize_tag_text(expected_name)
    if normalize_tag_text(tag.get("name") or "") == normalized_expected:
        return True
    return any(
        normalize_tag_text(str(alias)) == normalized_expected for alias in tag.get("aliases") or []
    )


def find_tag_by_name(client: StashClient, name: str) -> dict[str, Any] | None:
    for tag in search_tags(client, name):
        if matches_tag_name(tag, name):
            return tag
    return None


def ensure_root_tag(client: StashClient, name: str, dry_run: bool) -> dict[str, Any]:
    existing = find_tag_by_name(client, name)
    if existing:
        return existing

    created = create_tag(client, name, [], dry_run)
    if created:
        return {**created, "children": []}

    raise RuntimeError(f"Root tag not found and cannot be created in dry-run mode: {name}")


def ensure_child_tag(
    client: StashClient,
    parent: dict[str, Any],
    child_name: str,
    dry_run: bool,
) -> dict[str, Any]:
    for child in parent.get("children") or []:
        if matches_tag_name(child, child_name):
            return child

    created = create_tag(client, child_name, [parent["id"]], dry_run)
    if created:
        child = {**created, "children": []}
        parent.setdefault("children", []).append(child)
        return child

    raise RuntimeError(
        f"Child tag not found and cannot be created in dry-run mode: {child_name} (parent: {parent.get('name')})"
    )


def extract_language_name(tags: list[str]) -> str | None:
    eht_language_tag = next((tag for tag in tags if str(tag).startswith("language:")), None)
    if not eht_language_tag:
        return None

    language_name = normalize_tag_text(
        eht_language_tag.split(":", 1)[1] if ":" in eht_language_tag else ""
    )
    return language_name or None


def find_matching_language_tag(
    tags: list[str],
    language_tags: list[dict[str, Any]],
) -> dict[str, Any] | None:
    language_name = extract_language_name(tags)
    if not language_name:
        return None

    subtitle_alias = f"{to_title_case(language_name)} Subtitle"
    for language_tag in language_tags:
        subtitle = language_tag.get("subtitle")
        if not subtitle:
            continue
        if matches_tag_name(subtitle, subtitle_alias):
            return subtitle
    return None


def find_matching_language_parent(
    tags: list[str],
    language_tags: list[dict[str, Any]],
) -> dict[str, Any] | None:
    language_name = extract_language_name(tags)
    if not language_name:
        return None

    for language_tag in language_tags:
        if matches_tag_name(language_tag, language_name):
            return language_tag
    return None


def load_tag_taxonomy(
    client: StashClient,
    dry_run: bool,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[str]]:
    audience_root_tag = ensure_root_tag(client, AUDIENCE_ROOT_TAG_NAME, dry_run)
    ensure_child_tag(client, audience_root_tag, AUDIENCE_TAG_NAME_GAY, dry_run)
    ensure_child_tag(client, audience_root_tag, AUDIENCE_TAG_NAME_STRAIGHT, dry_run)

    language_root_tag = ensure_root_tag(client, LANGUAGE_ROOT_TAG_NAME, dry_run)

    language_tags = []
    for child in language_root_tag.get("children") or []:
        descendants = [
            {
                "id": grandchild["id"],
                "name": grandchild.get("name"),
                "aliases": grandchild.get("aliases") or [],
            }
            for grandchild in child.get("children") or []
        ]
        subtitle = next(
            (
                grandchild
                for grandchild in descendants
                if normalize_tag_text(grandchild.get("name") or "").endswith(" subtitle")
                or any(
                    normalize_tag_text(str(alias)).endswith(" subtitle")
                    for alias in grandchild.get("aliases") or []
                )
            ),
            None,
        )
        language_tags.append(
            {
                "id": child["id"],
                "name": child.get("name"),
                "aliases": child.get("aliases") or [],
                "subtitle": subtitle,
                "descendants": descendants,
            }
        )

    language_tag_ids = []
    for tag in language_tags:
        language_tag_ids.append(tag["id"])
        language_tag_ids.extend(descendant["id"] for descendant in tag.get("descendants") or [])

    return audience_root_tag, language_root_tag, language_tags, language_tag_ids


def collect_language_tag_ids(language_tags: list[dict[str, Any]]) -> list[str]:
    language_tag_ids: list[str] = []
    for tag in language_tags:
        language_tag_ids.append(tag["id"])
        language_tag_ids.extend(descendant["id"] for descendant in tag.get("descendants") or [])
    return language_tag_ids


def gallery_log_prefix(gallery: dict[str, Any], gid: int | None = None, token: str | None = None) -> str:
    folder = (gallery.get("folder") or {}).get("path") or "<no-folder>"
    gallery_id = gallery.get("id") or "<no-id>"
    gid_token = f" gid_token={gid}_{token}" if gid is not None and token is not None else ""
    return f"gallery_id={gallery_id} folder={folder}{gid_token}"


def emit_gallery_logs(gallery: dict[str, Any], logs: list[str], gid: int, token: str) -> None:
    prefix = gallery_log_prefix(gallery, gid, token)
    for message in logs:
        emit_info(f"{prefix} {message}")


def resolve_audience_tag_id(
    client: StashClient,
    audience_root_tag: dict[str, Any],
    tags: list[str],
    dry_run: bool,
    logs: list[str],
) -> str:
    target_name = resolve_audience_tag_name(tags)
    logs.append(f"Audience tag resolved to {target_name}")
    return ensure_child_tag(client, audience_root_tag, target_name, dry_run)["id"]


def resolve_language_tag_id(
    client: StashClient,
    tags: list[str],
    language_root_tag: dict[str, Any],
    language_tags: list[dict[str, Any]],
    dry_run: bool,
    logs: list[str],
) -> str | None:
    matched = find_matching_language_tag(tags, language_tags)
    if matched:
        logs.append(f"Matched existing language subtitle tag: {matched.get('name')}")
        return matched["id"]

    language_name = extract_language_name(tags)
    if not language_name:
        return None

    subtitle_alias = f"{to_title_case(language_name)} Subtitle"
    language_parent = find_matching_language_parent(tags, language_tags)

    if not language_parent:
        parent_name = to_title_case(language_name)
        logs.append(f"Creating language parent tag: {parent_name}")
        language_parent = ensure_child_tag(client, language_root_tag, parent_name, dry_run)
        language_parent["name"] = language_parent.get("name") or parent_name
        language_parent["subtitle"] = None
        language_parent["descendants"] = [
            {
                "id": grandchild["id"],
                "name": grandchild.get("name"),
                "aliases": grandchild.get("aliases") or [],
            }
            for grandchild in language_parent.get("children") or []
        ]
        language_tags.append(language_parent)
    else:
        logs.append(f"Matched existing language parent tag: {language_parent.get('name')}")

    logs.append(f"Creating language subtitle tag: {subtitle_alias}")
    created_subtitle = create_tag(client, subtitle_alias, [language_parent["id"]], dry_run)
    if not created_subtitle:
        return None

    subtitle = {
        "id": created_subtitle["id"],
        "name": created_subtitle.get("name"),
        "aliases": created_subtitle.get("aliases") or [],
    }
    language_parent["subtitle"] = subtitle
    language_parent.setdefault("descendants", []).append(subtitle)
    return created_subtitle["id"]


def sync_gallery_metadata(
    client: StashClient,
    gallery: dict[str, Any],
    gid: int,
    token: str,
    audience_root_tag: dict[str, Any],
    language_tag_ids: list[str],
    language_root_tag: dict[str, Any],
    language_tags: list[dict[str, Any]],
    dry_run: bool,
) -> dict[str, Any]:
    emit_info(f"{gallery_log_prefix(gallery, gid, token)} Starting metadata sync")
    meta, logs = fetch_ehentai(gid, token)
    language_tag_ids = collect_language_tag_ids(language_tags)
    language_leaf_tag_ids = {
        descendant["id"]
        for language_tag in language_tags
        for descendant in language_tag.get("descendants") or []
    }

    existing_tag_ids = [tag["id"] for tag in gallery.get("tags") or []]
    audience_tag_ids = [child["id"] for child in audience_root_tag.get("children") or []]
    next_tag_ids = {
        tag_id
        for tag_id in existing_tag_ids
        if tag_id not in audience_tag_ids
        and (tag_id not in language_tag_ids or tag_id in language_leaf_tag_ids)
    }

    next_tag_ids.add(
        resolve_audience_tag_id(client, audience_root_tag, meta.get("tags") or [], dry_run, logs)
    )
    language_tag_id = resolve_language_tag_id(
        client=client,
        tags=meta.get("tags") or [],
        language_root_tag=language_root_tag,
        language_tags=language_tags,
        dry_run=dry_run,
        logs=logs,
    )
    if language_tag_id:
        next_tag_ids.add(language_tag_id)
        logs.append(f"Language subtitle tag resolved to id={language_tag_id}")
    elif not any(str(tag).startswith("language:") for tag in meta.get("tags") or []):
        logs.append("No language tag matched; the gallery will still be marked organized")

    payload = {
        "title": meta["title"],
        "date": posted_to_date(meta["posted"]),
        "rating100": int(round(float(meta["rating"])) * 20),
        "url": f"https://e-hentai.org/g/{gid}/{token}",
        "organized": True,
        "tag_ids": sorted(next_tag_ids),
    }

    updated = update_gallery(client, gallery["id"], payload, dry_run)
    emit_gallery_logs(gallery, logs, gid, token)
    action = "Prepared dry-run update for" if dry_run else "Updated"
    emit_info(
        f"{gallery_log_prefix(gallery, gid, token)} {action} title={meta['title']} "
        f"date={payload['date']} rating100={payload['rating100']} tags={len(payload['tag_ids'])}"
    )
    return {
        "id": gallery["id"],
        "path": gallery["folder"]["path"],
        "title": meta["title"],
        "dry_run": dry_run,
        "payload": payload,
        "logs": logs,
        "updated": updated,
    }


def select_target_galleries(
    client: StashClient,
    args: dict[str, Any],
) -> list[dict[str, Any]]:
    path_contains = args.get("path_contains")
    skip_organized = bool_arg(args.get("skip_organized"), True)
    hook_context = args.get("hookContext")

    if not hook_context:
        return find_galleries(client, path_contains, skip_organized)

    if hook_context.get("input") is not None:
        emit_info("Hook invocation ignored because hookContext.input was present")
        return []

    gallery = find_gallery_by_id(client, str(hook_context["id"]))
    if not gallery:
        emit_warn(f"Hook target gallery was not found: id={hook_context.get('id')}")
        return []
    if path_contains and path_contains not in (gallery.get("folder") or {}).get("path", ""):
        emit_info(f"{gallery_log_prefix(gallery)} Hook target skipped because path filter did not match")
        return []
    if skip_organized and gallery.get("organized"):
        emit_info(f"{gallery_log_prefix(gallery)} Hook target skipped because it is already organized")
        return []
    return [gallery]


def collect_targets(
    galleries: list[dict[str, Any]],
) -> tuple[list[tuple[dict[str, Any], int, str]], list[dict[str, Any]]]:
    targets: list[tuple[dict[str, Any], int, str]] = []
    skipped: list[dict[str, Any]] = []

    for gallery in galleries:
        folder = gallery.get("folder") or {}
        parsed = parse_gallery_dir(folder.get("path"))
        if not parsed:
            skipped.append(
                {
                    "id": gallery["id"],
                    "path": folder.get("path"),
                    "reason": "folder name is not gid_token",
                }
            )
            continue

        gid, token = parsed
        targets.append((gallery, gid, token))

    return targets, skipped


def process_targets(
    client: StashClient,
    targets: list[tuple[dict[str, Any], int, str]],
    audience_root_tag: dict[str, Any],
    language_tag_ids: list[str],
    language_root_tag: dict[str, Any],
    language_tags: list[dict[str, Any]],
    dry_run: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    results: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    total = len(targets)

    if total == 0:
        emit_progress(1.0)
        emit_info("No eligible galleries to process")
        return results, failed

    emit_progress(0.0)
    for index, (gallery, gid, token) in enumerate(targets, start=1):
        try:
            result = sync_gallery_metadata(
                client=client,
                gallery=gallery,
                gid=gid,
                token=token,
                audience_root_tag=audience_root_tag,
                language_tag_ids=language_tag_ids,
                language_root_tag=language_root_tag,
                language_tags=language_tags,
                dry_run=dry_run,
            )
            results.append(result)
        except (HTTPError, URLError, TimeoutError, RuntimeError, ValueError, KeyError) as exc:
            folder = gallery.get("folder") or {}
            emit_warn(
                f"{gallery_log_prefix(gallery, gid, token)} Metadata sync failed: {exc}"
            )
            failed.append(
                {
                    "id": gallery["id"],
                    "path": folder.get("path"),
                    "reason": str(exc),
                }
            )
        emit_progress(index / total)

    return results, failed
