import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from plugin_runtime import build_cookie_header


@dataclass
class StashClient:
    graphql_url: str
    api_key: str | None
    session_cookie: dict[str, Any] | None

    def request(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        # 延迟导入：urllib.request 启动开销约 100ms，被跳过的 hook 用不到它。
        from urllib.request import Request, urlopen

        payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
        parsed_url = urlsplit(self.graphql_url)
        origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/graphql-response+json, application/json",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0.0.0 Safari/537.36"
            ),
            "Origin": origin,
            "Referer": f"{origin}/",
        }
        if self.api_key:
            headers["ApiKey"] = self.api_key

        cookie_header = build_cookie_header(self.session_cookie)
        if cookie_header:
            headers["Cookie"] = cookie_header

        req = Request(self.graphql_url, data=payload, headers=headers, method="POST")
        with urlopen(req, timeout=60) as response:
            data = json.load(response)

        if data.get("errors"):
            raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False))

        return data["data"]


def _normalize_item(item: dict[str, Any], paths: list[str | None] | None = None) -> dict[str, Any]:
    normalized = dict(item)
    normalized["tag_ids"] = [str(tag["id"]) for tag in item.get("tags") or []]
    normalized["paths"] = [path for path in paths or [] if path]
    return normalized


TAG_TREE_DEPTH = 5


def _tag_tree_fields(depth: int) -> str:
    fields = "id name aliases custom_fields"
    if depth == 0:
        return fields
    return f"{fields} children {{ {_tag_tree_fields(depth - 1)} }}"


def find_root_tags(client: StashClient, names: list[str]) -> list[dict[str, Any]]:
    data = client.request(
        f"""
        query FindRootTags($tagFilter: TagFilterType, $filter: FindFilterType) {{
          findTags(tag_filter: $tagFilter, filter: $filter) {{
            tags {{
              {_tag_tree_fields(TAG_TREE_DEPTH)}
            }}
          }}
        }}
        """,
        {
            "tagFilter": {
                "name": {
                    "value": f"^({'|'.join(map(re.escape, names))})$",
                    "modifier": "MATCHES_REGEX",
                },
                "parent_count": {"value": 0, "modifier": "EQUALS"},
            },
            "filter": {"per_page": -1},
        },
    )
    return data["findTags"]["tags"]


def find_galleries(client: StashClient, ids: list[str] | None = None) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindGalleries($filter: FindFilterType, $ids: [ID!]) {
          findGalleries(filter: $filter, ids: $ids) {
            galleries {
              id
              title
              folder {
                path
              }
              files {
                path
              }
              tags {
                id
                name
              }
            }
          }
        }
        """,
        {"filter": {"per_page": -1}, "ids": ids},
    )
    return [
        _normalize_item(
            item,
            [
                (item.get("folder") or {}).get("path"),
                *(file.get("path") for file in item.get("files") or []),
            ],
        )
        for item in data["findGalleries"]["galleries"]
    ]


def find_groups(client: StashClient, ids: list[str] | None = None) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindGroups($filter: FindFilterType, $ids: [ID!]) {
          findGroups(filter: $filter, ids: $ids) {
            groups {
              id
              name
              tags {
                id
                name
              }
            }
          }
        }
        """,
        {"filter": {"per_page": -1}, "ids": ids},
    )
    return [_normalize_item(item) for item in data["findGroups"]["groups"]]


def find_images(client: StashClient, ids: list[str] | None = None) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindImages($filter: FindFilterType, $ids: [ID!]) {
          findImages(filter: $filter, ids: $ids) {
            images {
              id
              title
              visual_files {
                ... on ImageFile {
                  path
                }
              }
              tags {
                id
                name
              }
            }
          }
        }
        """,
        {"filter": {"per_page": -1}, "ids": ids},
    )
    return [
        _normalize_item(
            item,
            [file.get("path") for file in item.get("visual_files") or []],
        )
        for item in data["findImages"]["images"]
    ]


def find_scenes(client: StashClient, ids: list[str] | None = None) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindScenes($filter: FindFilterType, $ids: [ID!]) {
          findScenes(filter: $filter, ids: $ids) {
            scenes {
              id
              title
              files {
                path
              }
              tags {
                id
                name
              }
            }
          }
        }
        """,
        {"filter": {"per_page": -1}, "ids": ids},
    )
    return [
        _normalize_item(item, [file.get("path") for file in item.get("files") or []])
        for item in data["findScenes"]["scenes"]
    ]


def find_performers(client: StashClient, ids: list[str] | None = None) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindPerformers($filter: FindFilterType, $ids: [ID!]) {
          findPerformers(filter: $filter, ids: $ids) {
            performers {
              id
              name
              rating100
              tags {
                id
                name
              }
            }
          }
        }
        """,
        {"filter": {"per_page": -1}, "ids": ids},
    )
    return [_normalize_item(item) for item in data["findPerformers"]["performers"]]


def update_gallery_tags(client: StashClient, gallery_id: str, tag_ids: list[str]) -> dict[str, Any]:
    data = client.request(
        """
        mutation GalleryUpdate($input: GalleryUpdateInput!) {
          galleryUpdate(input: $input) {
            id
            title
          }
        }
        """,
        {"input": {"id": gallery_id, "tag_ids": tag_ids}},
    )
    updated = data.get("galleryUpdate")
    if not updated:
        raise RuntimeError(f"Failed to update gallery: {gallery_id}")
    return updated


def bulk_update_gallery_tags(
    client: StashClient, gallery_ids: list[str], tag_ids: list[str]
) -> list[dict[str, Any]]:
    data = client.request(
        """
        mutation BulkGalleryUpdate($input: BulkGalleryUpdateInput!) {
          bulkGalleryUpdate(input: $input) {
            id
            title
          }
        }
        """,
        {"input": {"ids": gallery_ids, "tag_ids": {"ids": tag_ids, "mode": "SET"}}},
    )
    return data.get("bulkGalleryUpdate") or []


def bulk_update_group_tags(
    client: StashClient, group_ids: list[str], tag_ids: list[str]
) -> list[dict[str, Any]]:
    data = client.request(
        """
        mutation BulkGroupUpdate($input: BulkGroupUpdateInput!) {
          bulkGroupUpdate(input: $input) {
            id
            name
          }
        }
        """,
        {"input": {"ids": group_ids, "tag_ids": {"ids": tag_ids, "mode": "SET"}}},
    )
    return data.get("bulkGroupUpdate") or []


def bulk_update_image_tags(
    client: StashClient, image_ids: list[str], tag_ids: list[str]
) -> list[dict[str, Any]]:
    data = client.request(
        """
        mutation BulkImageUpdate($input: BulkImageUpdateInput!) {
          bulkImageUpdate(input: $input) {
            id
            title
          }
        }
        """,
        {"input": {"ids": image_ids, "tag_ids": {"ids": tag_ids, "mode": "SET"}}},
    )
    return data.get("bulkImageUpdate") or []


def update_scene_tags(client: StashClient, scene_id: str, tag_ids: list[str]) -> dict[str, Any]:
    data = client.request(
        """
        mutation SceneUpdate($input: SceneUpdateInput!) {
          sceneUpdate(input: $input) {
            id
            title
          }
        }
        """,
        {"input": {"id": scene_id, "tag_ids": tag_ids}},
    )
    updated = data.get("sceneUpdate")
    if not updated:
        raise RuntimeError(f"Failed to update scene: {scene_id}")
    return updated


def bulk_update_scene_tags(
    client: StashClient, scene_ids: list[str], tag_ids: list[str]
) -> list[dict[str, Any]]:
    data = client.request(
        """
        mutation BulkSceneUpdate($input: BulkSceneUpdateInput!) {
          bulkSceneUpdate(input: $input) {
            id
            title
          }
        }
        """,
        {"input": {"ids": scene_ids, "tag_ids": {"ids": tag_ids, "mode": "SET"}}},
    )
    return data.get("bulkSceneUpdate") or []


def update_performer_tags(
    client: StashClient, performer_id: str, tag_ids: list[str]
) -> dict[str, Any]:
    data = client.request(
        """
        mutation PerformerUpdate($input: PerformerUpdateInput!) {
          performerUpdate(input: $input) {
            id
            name
          }
        }
        """,
        {"input": {"id": performer_id, "tag_ids": tag_ids}},
    )
    updated = data.get("performerUpdate")
    if not updated:
        raise RuntimeError(f"Failed to update performer: {performer_id}")
    return updated


def bulk_update_performer_tags(
    client: StashClient, performer_ids: list[str], tag_ids: list[str]
) -> list[dict[str, Any]]:
    data = client.request(
        """
        mutation BulkPerformerUpdate($input: BulkPerformerUpdateInput!) {
          bulkPerformerUpdate(input: $input) {
            id
            name
          }
        }
        """,
        {"input": {"ids": performer_ids, "tag_ids": {"ids": tag_ids, "mode": "SET"}}},
    )
    return data.get("bulkPerformerUpdate") or []
