import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen

from plugin_runtime import build_cookie_header

PLUGIN_ID = "compress-performer-covers"


@dataclass
class StashClient:
    graphql_url: str
    api_key: str | None
    session_cookie: dict[str, Any] | None

    def _headers(self) -> dict[str, str]:
        headers = {"User-Agent": "stash-compress-performer-covers/0.1"}
        if self.api_key:
            headers["ApiKey"] = self.api_key

        cookie_header = build_cookie_header(self.session_cookie)
        if cookie_header:
            headers["Cookie"] = cookie_header
        return headers

    def request(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
        headers = {"Content-Type": "application/json", **self._headers()}
        request = Request(self.graphql_url, data=payload, headers=headers, method="POST")
        with urlopen(request, timeout=60) as response:
            data = json.load(response)

        if data.get("errors"):
            raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False))
        return data["data"]

    def image_url(self, image_path: str) -> str:
        if image_path.lower().startswith(("http://", "https://")):
            return image_path

        parsed = urlsplit(self.graphql_url)
        origin = f"{parsed.scheme}://{parsed.netloc}/"
        return urljoin(origin, image_path)

    def image_size(self, image_path: str) -> int | None:
        request = Request(self.image_url(image_path), headers=self._headers(), method="HEAD")
        try:
            with urlopen(request, timeout=30) as response:
                value = response.headers.get("Content-Length")
        except OSError:
            return None

        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def download_image(self, image_path: str) -> bytes:
        request = Request(self.image_url(image_path), headers=self._headers())
        with urlopen(request, timeout=30) as response:
            return response.read()


def find_performers(client: StashClient) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindPerformers($filter: FindFilterType) {
          findPerformers(filter: $filter) {
            performers {
              id
              name
              image_path
            }
          }
        }
        """,
        {"filter": {"per_page": -1, "sort": "name", "direction": "ASC"}},
    )
    return data["findPerformers"]["performers"]


def find_performer(client: StashClient, performer_id: str) -> dict[str, Any] | None:
    data = client.request(
        """
        query FindPerformer($id: ID!) {
          findPerformer(id: $id) {
            id
            name
            image_path
          }
        }
        """,
        {"id": performer_id},
    )
    return data.get("findPerformer")


def get_runtime_config(client: StashClient) -> dict[str, Any]:
    data = client.request(
        """
        query RuntimeConfig($pluginIds: [ID!]) {
          systemStatus {
            ffmpegPath
            ffprobePath
            os
          }
          configuration {
            plugins(include: $pluginIds)
          }
        }
        """,
        {"pluginIds": [PLUGIN_ID]},
    )
    plugin_configs = data["configuration"].get("plugins") or {}
    return {
        "system": data["systemStatus"],
        "settings": plugin_configs.get(PLUGIN_ID) or {},
    }


def update_performer_image(client: StashClient, performer_id: str, data_url: str) -> None:
    data = client.request(
        """
        mutation PerformerUpdate($input: PerformerUpdateInput!) {
          performerUpdate(input: $input) {
            id
          }
        }
        """,
        {"input": {"id": performer_id, "image": data_url}},
    )
    if not data.get("performerUpdate"):
        raise RuntimeError(f"Failed to update the cover for performer {performer_id}")
