import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from plugin_runtime import build_cookie_header

IMDB_PATH_REGEX = r"\[imdbid=tt\d+\]"
# Root tag of the Group types, shared with extended-attributes.
GROUP_ROOT_TAG = "__GROUP__"

SCENE_FIELDS = """
  id
  title
  urls
  files {
    path
  }
  groups {
    group {
      id
    }
    scene_index
  }
"""

SCRAPED_GROUP_FIELDS = "name aliases duration date director urls synopsis front_image back_image"
SCRAPED_SCENE_FIELDS = "title code details director urls date image"


@dataclass
class StashClient:
    graphql_url: str
    api_key: str | None
    session_cookie: dict[str, Any] | None

    def request(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        # 延迟导入：urllib.request 启动开销约 100ms，无需处理的 hook 用不到它。
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
        # 刮削器要抓网页和封面，耗时可能超过一分钟。
        with urlopen(req, timeout=180) as response:
            data = json.load(response)

        if data.get("errors"):
            raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False))

        return data["data"]


def find_scenes(
    client: StashClient, ids: list[str] | None = None, imdb_ids: list[str] | None = None
) -> list[dict[str, Any]]:
    path_regex = rf"\[imdbid=({'|'.join(imdb_ids)})\]" if imdb_ids else IMDB_PATH_REGEX
    data = client.request(
        f"""
        query FindScenes($sceneFilter: SceneFilterType, $filter: FindFilterType, $ids: [ID!]) {{
          findScenes(scene_filter: $sceneFilter, filter: $filter, ids: $ids) {{
            scenes {{
              {SCENE_FIELDS}
            }}
          }}
        }}
        """,
        {
            "sceneFilter": {"path": {"value": path_regex, "modifier": "MATCHES_REGEX"}},
            "filter": {"per_page": -1},
            "ids": ids,
        },
    )
    return data["findScenes"]["scenes"]


def find_groups(client: StashClient, group_filter: dict[str, Any]) -> list[dict[str, Any]]:
    data = client.request(
        """
        query FindGroups($groupFilter: GroupFilterType, $filter: FindFilterType) {
          findGroups(group_filter: $groupFilter, filter: $filter) {
            groups {
              id
              name
            }
          }
        }
        """,
        {"groupFilter": group_filter, "filter": {"per_page": -1}},
    )
    return data["findGroups"]["groups"]


def find_group_subtypes(client: StashClient) -> list[dict[str, Any]]:
    """The subtype tags directly under the `__GROUP__` root tag."""
    data = client.request(
        """
        query FindGroupSubtypes($tagFilter: TagFilterType!) {
          findTags(tag_filter: $tagFilter, filter: { per_page: -1 }) {
            tags {
              children {
                id
                name
                aliases
              }
            }
          }
        }
        """,
        {
            "tagFilter": {
                "name": {"value": GROUP_ROOT_TAG, "modifier": "EQUALS"},
                "parents": {"value": [], "modifier": "IS_NULL"},
            }
        },
    )
    return [child for tag in data["findTags"]["tags"] for child in tag["children"]]


def create_group(client: StashClient, group_input: dict[str, Any]) -> dict[str, Any]:
    data = client.request(
        """
        mutation GroupCreate($input: GroupCreateInput!) {
          groupCreate(input: $input) {
            id
            name
          }
        }
        """,
        {"input": group_input},
    )
    return data["groupCreate"]


def update_scene(client: StashClient, scene_input: dict[str, Any]) -> None:
    client.request(
        """
        mutation SceneUpdate($input: SceneUpdateInput!) {
          sceneUpdate(input: $input) {
            id
          }
        }
        """,
        {"input": scene_input},
    )


def scrape_group_url(client: StashClient, url: str) -> dict[str, Any] | None:
    data = client.request(
        f"""
        query ScrapeGroup($url: String!) {{
          scrapeGroupURL(url: $url) {{ {SCRAPED_GROUP_FIELDS} }}
        }}
        """,
        {"url": url},
    )
    return data["scrapeGroupURL"]


def scrape_scene_url(client: StashClient, url: str) -> dict[str, Any] | None:
    data = client.request(
        f"""
        query ScrapeScene($url: String!) {{
          scrapeSceneURL(url: $url) {{ {SCRAPED_SCENE_FIELDS} }}
        }}
        """,
        {"url": url},
    )
    return data["scrapeSceneURL"]
