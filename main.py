#!/usr/bin/env python3
"""Remote-viewing document ingestion pipeline built with Microsoft Agent Framework workflows.

Node 1 (``extract_source``) downloads the AIR evaluation (1995) PDF
("An Evaluation of Remote Viewing: Research and Applications", American
Institutes for Research, 183 pages) from the National Security Archive and
stores it, with provenance metadata, in Azure Blob Storage:

    https://uapstorage1.blob.core.windows.net/remote-viewing-docs-air-evaluation-1995

Authentication to Blob Storage uses ``DefaultAzureCredential`` (``az login``
locally, managed identity in Azure). The identity needs the
"Storage Blob Data Contributor" role on the storage account or container.

The graph is built with ``WorkflowBuilder`` so further nodes (content
understanding, cleanup, chunking, indexing) can be chained after node 1 with
``.add_edge(extract_source, <next_node>)``.

See: https://learn.microsoft.com/en-gb/agent-framework/get-started/workflows
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx
from agent_framework import Executor, Workflow, WorkflowBuilder, WorkflowContext, handler
from azure.core.exceptions import AzureError, ResourceExistsError
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import BlobServiceClient
from dotenv import load_dotenv
from typing_extensions import Never

LOGGER = logging.getLogger("rv_ingest_pipeline")

DEFAULT_SOURCE_URL = "https://nsarchive2.gwu.edu/NSAEBB/NSAEBB54/docs/doc_57.pdf"
DEFAULT_ACCOUNT_URL = "https://uapstorage1.blob.core.windows.net"
DEFAULT_CONTAINER = "remote-viewing-docs-air-evaluation-1995"
DEFAULT_BLOB_NAME = "source/air-evaluation-1995.pdf"
USER_AGENT = "rv-ingest-pipeline/0.1 (+https://github.com/indrajitbhas)"


@dataclass(frozen=True)
class AppConfig:
    """Runtime configuration for the CLI."""

    source_url: str
    storage_account_url: str
    container: str
    blob_name: str
    force: bool
    timeout_seconds: float


@dataclass(frozen=True)
class SourceDocument:
    """Describes the document to extract (input to node 1)."""

    doc_id: str
    title: str
    source_url: str
    publisher: str
    year: int


@dataclass(frozen=True)
class ExtractOutcome:
    """Result of node 1: where the PDF landed in Blob Storage."""

    doc_id: str
    status: str  # "uploaded" | "skipped" | "failed"
    blob_url: str
    size_bytes: int
    sha256: str
    elapsed_seconds: float
    message: str = ""


AIR_EVALUATION_1995 = SourceDocument(
    doc_id="air-evaluation-1995",
    title="An Evaluation of Remote Viewing: Research and Applications",
    source_url=DEFAULT_SOURCE_URL,
    publisher="American Institutes for Research (via National Security Archive, NSAEBB54 doc 57)",
    year=1995,
)


def _ascii_meta(value: str) -> str:
    """Blob metadata must be ASCII; drop anything else."""
    return value.encode("ascii", "ignore").decode("ascii")


class ExtractSourceExecutor(Executor):
    """Node 1: download the source PDF and upload it to the Blob container.

    Idempotent: if the blob already exists (and ``--force`` is not set) the
    upload is skipped and the existing blob's properties are reported.
    """

    def __init__(self, id: str, blob_service_client: BlobServiceClient, config: AppConfig):
        super().__init__(id=id)
        self._blob_service_client = blob_service_client
        self._config = config

    async def _download(self, url: str) -> bytes:
        async with httpx.AsyncClient(
            timeout=self._config.timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.content
        if not data.startswith(b"%PDF"):
            raise ValueError(f"Downloaded content from {url} is not a PDF (first bytes: {data[:8]!r})")
        return data

    @handler
    async def extract(self, doc: SourceDocument, ctx: WorkflowContext[Never, ExtractOutcome]) -> None:
        started = time.perf_counter()
        container = self._blob_service_client.get_container_client(self._config.container)
        blob_client = container.get_blob_client(self._config.blob_name)

        try:
            try:
                await container.create_container()
                LOGGER.info("Created container: %s", self._config.container)
            except ResourceExistsError:
                pass

            if not self._config.force and await blob_client.exists():
                props = await blob_client.get_blob_properties()
                LOGGER.info("Skipping (blob exists, use --force to overwrite): %s", blob_client.url)
                await ctx.yield_output(
                    ExtractOutcome(
                        doc_id=doc.doc_id,
                        status="skipped",
                        blob_url=blob_client.url,
                        size_bytes=props.size,
                        sha256=(props.metadata or {}).get("sha256", ""),
                        elapsed_seconds=time.perf_counter() - started,
                        message="blob already exists",
                    )
                )
                return

            LOGGER.info("Downloading %s", doc.source_url)
            data = await self._download(doc.source_url)
            digest = hashlib.sha256(data).hexdigest()
            LOGGER.info("Downloaded %d bytes (sha256=%s)", len(data), digest)

            metadata = {
                "doc_id": doc.doc_id,
                "title": _ascii_meta(doc.title),
                "publisher": _ascii_meta(doc.publisher),
                "year": str(doc.year),
                "source_url": doc.source_url,
                "sha256": digest,
                "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "pipeline_node": self.id,
            }
            await blob_client.upload_blob(
                data,
                overwrite=True,
                metadata=metadata,
                content_settings=ContentSettings(content_type="application/pdf"),
            )
            LOGGER.info("Uploaded to %s", blob_client.url)

            await ctx.yield_output(
                ExtractOutcome(
                    doc_id=doc.doc_id,
                    status="uploaded",
                    blob_url=blob_client.url,
                    size_bytes=len(data),
                    sha256=digest,
                    elapsed_seconds=time.perf_counter() - started,
                )
            )
        except (httpx.HTTPError, AzureError, ValueError) as exc:
            LOGGER.exception("Extraction failed for %s", doc.doc_id)
            await ctx.yield_output(
                ExtractOutcome(
                    doc_id=doc.doc_id,
                    status="failed",
                    blob_url=blob_client.url,
                    size_bytes=0,
                    sha256="",
                    elapsed_seconds=time.perf_counter() - started,
                    message=str(exc),
                )
            )


def build_workflow(blob_service_client: BlobServiceClient, config: AppConfig) -> Workflow:
    """Build the workflow graph. Currently: extract_source (node 1)."""
    extract_source = ExtractSourceExecutor("extract_source", blob_service_client, config)
    # Next nodes get chained here, e.g.:
    #   .add_edge(extract_source, analyze_document)
    return WorkflowBuilder(start_executor=extract_source).build()


def _create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Node 1: download the AIR evaluation (1995) PDF and store it in Azure Blob Storage."
    )
    parser.add_argument("--source-url", default=None, help="Override the source PDF URL.")
    parser.add_argument("--account-url", default=None, help="Storage account URL (AZURE_STORAGE_ACCOUNT_URL).")
    parser.add_argument("--container", default=None, help="Target container (BLOB_CONTAINER).")
    parser.add_argument("--blob-name", default=None, help="Target blob name (BLOB_NAME).")
    parser.add_argument("--force", action="store_true", help="Overwrite the blob if it already exists.")
    parser.add_argument("--timeout", type=float, default=120.0, help="HTTP download timeout in seconds.")
    return parser


def _build_config(args: argparse.Namespace) -> AppConfig:
    return AppConfig(
        source_url=args.source_url or os.getenv("SOURCE_URL") or DEFAULT_SOURCE_URL,
        storage_account_url=args.account_url or os.getenv("AZURE_STORAGE_ACCOUNT_URL") or DEFAULT_ACCOUNT_URL,
        container=args.container or os.getenv("BLOB_CONTAINER") or DEFAULT_CONTAINER,
        blob_name=args.blob_name or os.getenv("BLOB_NAME") or DEFAULT_BLOB_NAME,
        force=args.force or os.getenv("FORCE", "").lower() in {"1", "true", "yes"},
        timeout_seconds=args.timeout,
    )


async def run(config: AppConfig) -> int:
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=config.storage_account_url, credential=credential)
    LOGGER.info("Using DefaultAzureCredential for %s", config.storage_account_url)

    doc = SourceDocument(
        doc_id=AIR_EVALUATION_1995.doc_id,
        title=AIR_EVALUATION_1995.title,
        source_url=config.source_url,
        publisher=AIR_EVALUATION_1995.publisher,
        year=AIR_EVALUATION_1995.year,
    )

    try:
        workflow = build_workflow(blob_service_client, config)
        result = await workflow.run(doc)
        outcomes: list[ExtractOutcome] = result.get_outputs()

        print("Summary:")
        for o in outcomes:
            print(f"  {o.doc_id}: {o.status}")
            print(f"    blob:    {o.blob_url}")
            print(f"    size:    {o.size_bytes:,} bytes")
            if o.sha256:
                print(f"    sha256:  {o.sha256}")
            print(f"    elapsed: {o.elapsed_seconds:.1f}s")
            if o.message:
                print(f"    note:    {o.message}")
        return 1 if any(o.status == "failed" for o in outcomes) or not outcomes else 0
    finally:
        await blob_service_client.close()
        await credential.close()


async def main_async() -> int:
    load_dotenv(override=False)
    args = _create_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logging.getLogger("azure").setLevel(logging.WARNING)
    try:
        return await run(_build_config(args))
    except Exception:  # noqa: BLE001
        LOGGER.exception("Fatal error")
        return 3


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
