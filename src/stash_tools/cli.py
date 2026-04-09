from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from graphql import build_client_schema, get_introspection_query, print_schema


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    root = project_root()
    load_dotenv(root / ".env", override=False)
    parser = argparse.ArgumentParser(description="Download the Stash GraphQL schema")
    parser.add_argument(
        "--url",
        default=os.getenv("STASH_URL"),
        help="Base Stash URL, for example http://127.0.0.1:9999",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("STASH_KEY"),
        help="Stash API key. Falls back to the STASH_KEY environment variable.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "schema.graphql",
        help="Path to the output schema file.",
    )
    return parser.parse_args()


def fetch_introspection(stash_url: str, api_key: str | None) -> dict:
    normalized_url = stash_url.rstrip("/")
    headers = {
        "content-type": "application/json",
        "Accept": "application/graphql-response+json, application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Origin": normalized_url,
        "Referer": f"{normalized_url}/",
    }
    if api_key:
        headers["ApiKey"] = api_key

    request = Request(
        f"{normalized_url}/graphql",
        data=json.dumps({"query": get_introspection_query()}).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urlopen(request, timeout=60) as response:
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(f"Schema introspection failed: {response.status} {response.reason}")
        return json.load(response)


def render_schema(payload: dict) -> str:
    data = payload.get("data")
    if data is None:
        errors = payload.get("errors") or []
        messages = [
            error.get("message", "Unknown GraphQL introspection error")
            for error in errors
            if isinstance(error, dict)
        ]
        raise RuntimeError("; ".join(messages) or "Unknown GraphQL introspection error")

    schema = build_client_schema(data)
    return f"{print_schema(schema)}\n"


def main() -> None:
    args = parse_args()
    if not args.url:
        raise RuntimeError("Missing Stash URL. Pass --url or set STASH_URL.")

    try:
        payload = fetch_introspection(args.url, args.api_key)
    except HTTPError as exc:
        raise RuntimeError(f"Schema introspection failed: {exc.code} {exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"Schema introspection request failed: {exc.reason}") from exc

    schema_text = render_schema(payload)
    output_path = Path(args.output).resolve()
    output_path.write_text(schema_text, encoding="utf-8")
    print(f"Schema downloaded successfully: {output_path}")


if __name__ == "__main__":
    main()
