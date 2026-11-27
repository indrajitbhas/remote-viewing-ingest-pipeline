#!/usr/bin/env python3
"""Deploy the Azure AI Search definitions in ``azure-search/`` (keyless).

Order: data source -> index -> skillset -> indexer, then optionally reset + run
the indexer. Each JSON file contains ``__PLACEHOLDER__`` tokens that are filled
from environment variables (see ``.env.example``), so no endpoints, resource
IDs or keys are committed.

Auth: ``DefaultAzureCredential`` (``az login``). Your identity needs the
"Search Service Contributor" role, and the search service must allow RBAC
("Role-based access control" or "Both" under Settings > Keys).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

LOGGER = logging.getLogger("deploy_search")
API_VERSION = "2026-04-01"
SEARCH_SCOPE = "https://search.azure.com/.default"
DEFINITIONS_DIR = Path(__file__).parent / "azure-search"

# placeholder -> (env var, default)
PLACEHOLDERS: dict[str, tuple[str, str | None]] = {
    "SEARCH_INDEX": ("SEARCH_INDEX_NAME", "remote-viewing-index"),
    "SEARCH_DATASOURCE": ("SEARCH_DATASOURCE_NAME", "remote-viewing-blob-ds"),
    "SEARCH_SKILLSET": ("SEARCH_SKILLSET_NAME", "remote-viewing-skillset"),
    "SEARCH_INDEXER": ("SEARCH_INDEXER_NAME", "remote-viewing-indexer"),
    "BLOB_CONTAINER": ("BLOB_CONTAINER", "remote-viewing-docs-air-evaluation-1995"),
    "STORAGE_RESOURCE_ID": ("AZURE_STORAGE_RESOURCE_ID", None),
    "AOAI_ENDPOINT": ("AZURE_OPENAI_ENDPOINT", None),
    "EMBEDDING_DEPLOYMENT": ("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-3-small"),
    "EMBEDDING_MODEL": ("AZURE_OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
}

# (collection, file, placeholder holding the object name)
STEPS = [
    ("datasources", "datasource.json", "SEARCH_DATASOURCE"),
    ("indexes", "index.json", "SEARCH_INDEX"),
    ("skillsets", "skillset.json", "SEARCH_SKILLSET"),
    ("indexers", "indexer.json", "SEARCH_INDEXER"),
]


def normalize_endpoint(value: str, env: str) -> str:
    """Reduce an endpoint to scheme://host.

    Search skills/vectorizers append their own path (``/openai/deployments/...``),
    so a value like ``https://x.openai.azure.com/openai/v1`` produces a 404.
    """
    parts = urlsplit(value.strip())
    if not parts.scheme or not parts.netloc:
        raise ValueError(f"{env} is not a valid URL: {value!r}")
    base = f"{parts.scheme}://{parts.netloc}"
    if parts.path.strip("/"):
        LOGGER.warning("%s: dropping path '%s' -> using %s", env, parts.path, base)
    return base


def resolve_values() -> dict[str, str]:
    values, missing = {}, []
    for token, (env, default) in PLACEHOLDERS.items():
        value = (os.getenv(env) or default or "").strip()
        if not value:
            missing.append(env)
        if token.endswith("ENDPOINT") and value:
            value = normalize_endpoint(value, env)
        values[token] = value
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")
    return values


def render(path: Path, values: dict[str, str]) -> dict:
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"__([A-Z_]+)__", lambda m: values.get(m.group(1), m.group(0)), text)
    leftover = re.findall(r"__[A-Z_]+__", text)
    if leftover:
        raise ValueError(f"Unresolved placeholders in {path.name}: {sorted(set(leftover))}")
    return json.loads(text)


def report_status(client: httpx.Client, indexer: str, index: str, wait_seconds: int) -> int:
    """Poll the indexer until its last run finishes (or timeout), then print a summary."""
    deadline = time.monotonic() + wait_seconds
    while True:
        resp = client.get(f"/indexers/{indexer}/status")
        if resp.status_code >= 400:
            LOGGER.error("GET indexer status failed (%s): %s", resp.status_code, resp.text)
            return 1
        status = resp.json()
        last = status.get("lastResult") or {}
        state = last.get("status", "none")
        if state != "inProgress" or time.monotonic() >= deadline:
            break
        LOGGER.info("Indexer in progress (%s items processed)... waiting", last.get("itemsProcessed", 0))
        time.sleep(15)

    stats = client.get(f"/indexes/{index}/stats")
    doc_count = stats.json().get("documentCount", "?") if stats.status_code < 400 else "?"

    print(f"Indexer:         {indexer} (service status: {status.get('status')})")
    print(f"Last run:        {state}  {last.get('startTime', '')} -> {last.get('endTime', '')}")
    print(f"Items processed: {last.get('itemsProcessed', 0)}   failed: {last.get('itemsFailed', 0)}")
    print(f"Index chunks:    {doc_count}  ({index}; stats can lag a few minutes)")
    if last.get("errorMessage"):
        print(f"Run error:       {last['errorMessage']}")
    for kind in ("errors", "warnings"):
        items = last.get(kind) or []
        if items:
            print(f"{kind.capitalize()} ({len(items)}):")
            for item in items[:10]:
                print(f"  - [{item.get('name') or item.get('key', '')}] {item.get('message') or item.get('errorMessage', '')}")
    if state == "inProgress":
        print("Still running: rerun with --status to check again.")
        return 0
    return 0 if state == "success" and not last.get("errors") else 1


def main() -> int:
    load_dotenv(override=False)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run", action="store_true", help="Reset and run the indexer after deploying.")
    parser.add_argument("--dry-run", action="store_true", help="Print rendered definitions without calling Azure.")
    parser.add_argument(
        "--status", action="store_true", help="Skip deployment; wait for the indexer run and print the result."
    )
    parser.add_argument("--wait", type=int, default=600, help="Max seconds --status waits for a running indexer.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    try:
        values = resolve_values()
        rendered = [(c, render(DEFINITIONS_DIR / f, values), values[n]) for c, f, n in STEPS]
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 2

    if args.dry_run:
        for collection, body, name in rendered:
            print(f"--- PUT /{collection}/{name}")
            print(json.dumps(body, indent=2))
        return 0

    endpoint = (os.getenv("SEARCH_ENDPOINT") or "").rstrip("/")
    if not endpoint:
        LOGGER.error("Missing environment variable: SEARCH_ENDPOINT")
        return 2

    token = DefaultAzureCredential().get_token(SEARCH_SCOPE).token
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    params = {"api-version": API_VERSION}

    with httpx.Client(base_url=endpoint, headers=headers, params=params, timeout=60) as client:
        if args.status:
            return report_status(client, values["SEARCH_INDEXER"], values["SEARCH_INDEX"], args.wait)

        for collection, body, name in rendered:
            resp = client.put(f"/{collection}/{name}", json=body)
            if resp.status_code >= 400:
                LOGGER.error("PUT %s/%s failed (%s): %s", collection, name, resp.status_code, resp.text)
                return 1
            LOGGER.info("PUT %s/%s -> %s", collection, name, resp.status_code)

        if args.run:
            indexer = values["SEARCH_INDEXER"]
            for action in ("reset", "run"):
                resp = client.post(f"/indexers/{indexer}/{action}")
                if resp.status_code >= 400:
                    LOGGER.error("%s indexer failed (%s): %s", action, resp.status_code, resp.text)
                    return 1
                LOGGER.info("Indexer %s: %s", action, resp.status_code)
            print("Indexer started. Check the result with: python deploy_search.py --status")
    return 0


if __name__ == "__main__":
    sys.exit(main())
