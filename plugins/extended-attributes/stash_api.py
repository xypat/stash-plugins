import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from plugin_runtime import build_cookie_header


@dataclass
class StashClient:
    graphql_url: str
    api_key: str | None
    session_cookie: dict[str, Any] | None

    def request(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
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


def _normalize_tag_ids(item: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(item)
    normalized["tag_ids"] = [str(tag["id"]) for tag in item.get("tags") or []]
    return normalized


def find_root_tags(client: StashClient) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindRootTags($tagFilter: TagFilterType, $filter: FindFilterType) {
          findTags(tag_filter: $tagFilter, filter: $filter) {
            tags {
              id
              name
              aliases
              parent_count
              children {
                id
                name
                aliases
                parent_count
                children {
                  id
                  name
                  aliases
                  parent_count
                  children {
                    id
                    name
                    aliases
                    parent_count
                  }
                }
              }
            }
          }
        }
        """,
        {
            "tagFilter": {"parent_count": {"value": 0, "modifier": "EQUALS"}},
            "filter": {"per_page": -1},
        },
    )
    return data["findTags"]["tags"]


def find_galleries(client: StashClient) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindGalleries($filter: FindFilterType) {
          findGalleries(filter: $filter) {
            galleries {
              id
              title
              tags {
                id
                name
              }
            }
          }
        }
        """,
        {"filter": {"per_page": -1}},
    )
    return [_normalize_tag_ids(item) for item in data["findGalleries"]["galleries"]]


def find_gallery_by_id(client: StashClient, gallery_id: str) -> dict[str, Any] | None:
    data = client.request(
        """
        query FindGallery($id: ID!) {
          findGallery(id: $id) {
            id
            title
            tags {
              id
              name
            }
          }
        }
        """,
        {"id": gallery_id},
    )
    gallery = data.get("findGallery")
    return _normalize_tag_ids(gallery) if gallery else None


def find_scenes(client: StashClient) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindScenes($filter: FindFilterType) {
          findScenes(filter: $filter) {
            scenes {
              id
              title
              groups {
                group {
                  id
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
        {"filter": {"per_page": -1}},
    )
    return [_normalize_tag_ids(item) for item in data["findScenes"]["scenes"]]


def find_scene_by_id(client: StashClient, scene_id: str) -> dict[str, Any] | None:
    data = client.request(
        """
        query FindScene($id: ID!) {
          findScene(id: $id) {
            id
            title
            groups {
              group {
                id
              }
            }
            tags {
              id
              name
            }
          }
        }
        """,
        {"id": scene_id},
    )
    scene = data.get("findScene")
    return _normalize_tag_ids(scene) if scene else None


def find_performers_with_rating(client: StashClient) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindPerformers($performerFilter: PerformerFilterType, $filter: FindFilterType) {
          findPerformers(performer_filter: $performerFilter, filter: $filter) {
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
        {
            "performerFilter": {"rating100": {"value": 20, "modifier": "GREATER_THAN"}},
            "filter": {"per_page": -1},
        },
    )
    return [_normalize_tag_ids(item) for item in data["findPerformers"]["performers"]]


def find_performer_by_id(client: StashClient, performer_id: str) -> dict[str, Any] | None:
    data = client.request(
        """
        query FindPerformer($id: ID!) {
          findPerformer(id: $id) {
            id
            name
            rating100
            tags {
              id
              name
            }
          }
        }
        """,
        {"id": performer_id},
    )
    performer = data.get("findPerformer")
    return _normalize_tag_ids(performer) if performer else None


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
