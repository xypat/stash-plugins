import json
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen

from plugin_runtime import build_cookie_header


@dataclass
class StashClient:
    graphql_url: str
    api_key: str | None
    session_cookie: dict[str, Any] | None

    def request(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["ApiKey"] = self.api_key

        cookie_header = build_cookie_header(self.session_cookie)
        if cookie_header:
            headers["Cookie"] = cookie_header

        req = Request(self.graphql_url, data=payload, headers=headers, method="POST")
        with urlopen(req, timeout=30) as response:
            data = json.load(response)

        if data.get("errors"):
            raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False))

        return data["data"]


def find_tag(client: StashClient, tag_id: str) -> dict[str, Any]:
    data = client.request(
        """
        query FindTag($id: ID!) {
          findTag(id: $id) {
            id
            name
            aliases
            children {
              id
              name
              aliases
              children {
                id
                name
                aliases
              }
            }
          }
        }
        """,
        {"id": tag_id},
    )
    tag = data.get("findTag")
    if not tag:
        raise RuntimeError(f"Tag not found: {tag_id}")
    return tag


def search_tags(client: StashClient, query: str) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindTags($filter: FindFilterType) {
          findTags(filter: $filter) {
            tags {
              id
              name
              aliases
              children {
                id
                name
                aliases
                children {
                  id
                  name
                  aliases
                }
              }
            }
          }
        }
        """,
        {"filter": {"q": query, "per_page": -1}},
    )
    return data["findTags"]["tags"]


def create_tag(
    client: StashClient,
    name: str,
    parent_ids: list[str],
    dry_run: bool,
) -> dict[str, Any] | None:
    if dry_run:
        return None

    data = client.request(
        """
        mutation TagCreate($input: TagCreateInput!) {
          tagCreate(input: $input) {
            id
            name
            aliases
          }
        }
        """,
        {"input": {"name": name, "aliases": [name], "parent_ids": parent_ids}},
    )
    created = data.get("tagCreate")
    if not created:
        raise RuntimeError(f"Failed to create tag: {name}")
    return created


def update_gallery(
    client: StashClient,
    gallery_id: str,
    payload: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any] | None:
    if dry_run:
        return None

    data = client.request(
        """
        mutation GalleryUpdate($input: GalleryUpdateInput!) {
          galleryUpdate(input: $input) {
            id
            title
            organized
          }
        }
        """,
        {"input": {"id": gallery_id, **payload}},
    )
    updated = data.get("galleryUpdate")
    if not updated:
        raise RuntimeError(f"Failed to update gallery: {gallery_id}")
    return updated


def find_galleries(
    client: StashClient,
    path_contains: str | None,
    skip_organized: bool,
) -> list[dict[str, Any]]:
    gallery_filter: dict[str, Any] = {}
    if path_contains:
        gallery_filter["path"] = {"value": path_contains, "modifier": "INCLUDES"}
    if skip_organized:
        gallery_filter["organized"] = False

    data = client.request(
        """
        query FindGalleries($galleryFilter: GalleryFilterType, $filter: FindFilterType) {
          findGalleries(gallery_filter: $galleryFilter, filter: $filter) {
            galleries {
              id
              title
              organized
              folder {
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
        {"galleryFilter": gallery_filter or None, "filter": {"per_page": -1}},
    )
    return data["findGalleries"]["galleries"]


def find_gallery_by_id(client: StashClient, gallery_id: str) -> dict[str, Any] | None:
    data = client.request(
        """
        query FindGallery($id: ID!) {
          findGallery(id: $id) {
            id
            title
            organized
            folder {
              path
            }
            tags {
              id
              name
            }
          }
        }
        """,
        {"id": gallery_id},
    )
    return data.get("findGallery")
