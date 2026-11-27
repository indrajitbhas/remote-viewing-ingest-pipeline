#!/usr/bin/env python3
"""Remote-viewing document ingestion pipeline built with Microsoft Agent Framework workflows.

Node 1 (``extract_source``) downloads a source PDF (or reads a local file with
``--file``) and stores it, with provenance metadata, as ``source/<doc_id>.pdf``
in Azure Blob Storage. With no document flags it ingests the AIR evaluation
(1995) ("An Evaluation of Remote Viewing: Research and Applications", American
Institutes for Research, 183 pages) from the National Security Archive. Other
documents are ingested with ``--doc-id/--title/--publisher/--year``:

    https://uapstorage1.blob.core.windows.net/remote-viewing-docs-air-evaluation-1995

Authentication to Blob Storage uses ``DefaultAzureCredential`` (``az login``
locally, managed identity in Azure). The identity needs the
"Storage Blob Data Contributor" role on the storage account or container.

Node 2 (``split_parts``) reads that PDF back from Blob Storage and splits it
into one PDF per page, so search results can cite the exact page. Each page
is far below the Azure AI Search indexer's per-document extraction limit
(262,144 characters). Pages go to ``parts/<doc_id>/`` with an integer
``page_number`` metadata value; the search data source indexes that folder only.

The graph is built with ``WorkflowBuilder``:
``extract_source -> split_parts``.

See: https://learn.microsoft.com/en-gb/agent-framework/get-started/workflows
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import logging
import os
import re
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
from pypdf import PdfReader, PdfWriter
from typing_extensions import Never

LOGGER = logging.getLogger("rv_ingest_pipeline")

DEFAULT_SOURCE_URL = "https://nsarchive2.gwu.edu/NSAEBB/NSAEBB54/docs/doc_57.pdf"
DEFAULT_ACCOUNT_URL = "https://uapstorage1.blob.core.windows.net"
DEFAULT_CONTAINER = "remote-viewing-docs-air-evaluation-1995"
SOURCE_PREFIX = "source"
DOC_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")
PARTS_PREFIX = "parts"
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
    local_file: Optional[str] = None


@dataclass(frozen=True)
class SourceDocument:
    """Describes the document to extract (input to node 1)."""

    doc_id: str
    title: str
    source_url: str
    publisher: str
    year: Optional[int]


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


@dataclass(frozen=True)
class SourceStored:
    """Message node 1 -> node 2: the source PDF is in Blob Storage."""

    doc: SourceDocument
    blob_name: str
    sha256: str


@dataclass(frozen=True)
class PartInfo:
    blob_name: str
    page_number: int  # 1-based


@dataclass(frozen=True)
class SplitOutcome:
    """Result of node 2: the per-page parts that the indexer reads."""

    doc_id: str
    status: str  # "uploaded" | "skipped" | "failed"
    parts: tuple[PartInfo, ...]
    total_pages: int
    elapsed_seconds: float
    message: str = ""
    empty_text_pages: tuple[int, ...] = ()


def split_pdf(data: bytes) -> list[tuple[int, bytes, bool]]:
    """Split PDF bytes into one single-page PDF per page.

    Returns (page_number_1based, pdf_bytes, has_text). ``has_text`` is False when
    the page has no extractable text layer (typically a scanned image), which the
    indexer (``imageAction: none``) would index as empty.
    """
    reader = PdfReader(io.BytesIO(data))
    parts = []
    for i, page in enumerate(reader.pages, start=1):
        has_text = bool((page.extract_text() or "").strip())
        writer = PdfWriter()
        writer.add_page(page)
        buf = io.BytesIO()
        writer.write(buf)
        parts.append((i, buf.getvalue(), has_text))
    return parts


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


def _doc_metadata(doc: SourceDocument) -> dict[str, str]:
    """Document-level blob metadata shared by the source blob and every page blob."""
    meta = {
        "doc_id": doc.doc_id,
        "title": _ascii_meta(doc.title),
        "publisher": _ascii_meta(doc.publisher),
        "source_url": _ascii_meta(doc.source_url),
    }
    if doc.year is not None:
        meta["year"] = str(doc.year)
    return meta


class ExtractSourceExecutor(Executor):
    """Node 1: download the source PDF and upload it to the Blob container.

    Idempotent: if the blob already exists (and ``--force`` is not set) the
    upload is skipped and the existing blob's properties are reported.
    """

    def __init__(self, id: str, blob_service_client: BlobServiceClient, config: AppConfig):
        super().__init__(id=id)
        self._blob_service_client = blob_service_client
        self._config = config

    async def _load(self, doc: SourceDocument) -> bytes:
        if self._config.local_file:
            LOGGER.info("Reading local file %s", self._config.local_file)
            with open(self._config.local_file, "rb") as fh:
                data = fh.read()
            if not data.startswith(b"%PDF"):
                raise ValueError(f"{self._config.local_file} is not a PDF (first bytes: {data[:8]!r})")
            return data
        LOGGER.info("Downloading %s", doc.source_url)
        return await self._download(doc.source_url)

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
    async def extract(self, doc: SourceDocument, ctx: WorkflowContext[SourceStored, ExtractOutcome]) -> None:
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
                await ctx.send_message(
                    SourceStored(doc, self._config.blob_name, (props.metadata or {}).get("sha256", ""))
                )
                return

            data = await self._load(doc)
            digest = hashlib.sha256(data).hexdigest()
            LOGGER.info("Loaded %d bytes (sha256=%s)", len(data), digest)

            metadata = {
                **_doc_metadata(doc),
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
            await ctx.send_message(SourceStored(doc, self._config.blob_name, digest))
        except (httpx.HTTPError, AzureError, ValueError, OSError) as exc:
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


class SplitPartsExecutor(Executor):
    """Node 2: split the stored PDF into one PDF per page (exact page citations).

    Idempotent: skips if parts already exist for the doc (unless ``--force``,
    which deletes and rewrites them).
    """

    def __init__(self, id: str, blob_service_client: BlobServiceClient, config: AppConfig):
        super().__init__(id=id)
        self._blob_service_client = blob_service_client
        self._config = config

    @handler
    async def split(self, msg: SourceStored, ctx: WorkflowContext[Never, SplitOutcome]) -> None:
        started = time.perf_counter()
        doc = msg.doc
        container = self._blob_service_client.get_container_client(self._config.container)
        prefix = f"{PARTS_PREFIX}/{doc.doc_id}/"
        try:
            existing = [b async for b in container.list_blobs(name_starts_with=prefix, include=["metadata"])]
            if existing and not self._config.force:
                parts = tuple(
                    PartInfo(b.name, int((b.metadata or {}).get("page_number", 0)))
                    for b in sorted(existing, key=lambda b: b.name)
                )
                LOGGER.info("Skipping split (%d parts exist under %s, use --force)", len(parts), prefix)
                await ctx.yield_output(
                    SplitOutcome(doc.doc_id, "skipped", parts, max((p.page_number for p in parts), default=0),
                                 time.perf_counter() - started, "parts already exist")
                )
                return
            for b in existing:
                await container.delete_blob(b.name)

            data = await (await container.get_blob_client(msg.blob_name).download_blob()).readall()
            pieces = split_pdf(data)
            total_pages = len(pieces)
            pad = len(str(total_pages))
            parts = []
            empty: list[int] = []
            for page_number, pdf, has_text in pieces:
                name = f"{prefix}{doc.doc_id}-p{page_number:0{pad}d}.pdf"
                if not has_text:
                    empty.append(page_number)
                    LOGGER.warning("Page %d has no extractable text (scanned?); it will index empty", page_number)
                metadata = {
                    **_doc_metadata(doc),
                    "source_blob": msg.blob_name,
                    "source_sha256": msg.sha256,
                    "page_number": str(page_number),
                    "pipeline_node": self.id,
                }
                await container.upload_blob(
                    name, pdf, overwrite=True, metadata=metadata,
                    content_settings=ContentSettings(content_type="application/pdf"),
                )
                LOGGER.info("Uploaded page %d: %s", page_number, name)
                parts.append(PartInfo(name, page_number))
            await ctx.yield_output(
                SplitOutcome(doc.doc_id, "uploaded", tuple(parts), total_pages, time.perf_counter() - started,
                             empty_text_pages=tuple(empty))
            )
        except (AzureError, ValueError, OSError) as exc:
            LOGGER.exception("Split failed for %s", doc.doc_id)
            await ctx.yield_output(
                SplitOutcome(doc.doc_id, "failed", (), 0, time.perf_counter() - started, str(exc))
            )


def build_workflow(blob_service_client: BlobServiceClient, config: AppConfig) -> Workflow:
    """Build the workflow graph: extract_source (node 1) -> split_parts (node 2)."""
    extract_source = ExtractSourceExecutor("extract_source", blob_service_client, config)
    split_parts = SplitPartsExecutor("split_parts", blob_service_client, config)
    return WorkflowBuilder(start_executor=extract_source).add_edge(extract_source, split_parts).build()


def _create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Store a PDF in Azure Blob Storage, then split it into one PDF per page. "
            "Defaults to the AIR evaluation (1995)."
        )
    )
    doc = parser.add_argument_group("document (defaults: AIR evaluation 1995)")
    doc.add_argument("--source-url", default=None, help="Source PDF URL (SOURCE_URL).")
    doc.add_argument("--file", default=None, help="Local PDF path instead of downloading (SOURCE_FILE).")
    doc.add_argument("--doc-id", default=None, help="Document id, lowercase/digits/hyphens; used in blob paths (DOC_ID).")
    doc.add_argument("--title", default=None, help="Document title (DOC_TITLE).")
    doc.add_argument("--publisher", default=None, help="Publisher / provenance (DOC_PUBLISHER).")
    doc.add_argument("--year", type=int, default=None, help="Publication year (DOC_YEAR).")
    parser.add_argument("--account-url", default=None, help="Storage account URL (AZURE_STORAGE_ACCOUNT_URL).")
    parser.add_argument("--container", default=None, help="Target container (BLOB_CONTAINER).")
    parser.add_argument("--blob-name", default=None, help="Source blob name (BLOB_NAME, default source/<doc_id>.pdf).")
    parser.add_argument("--force", action="store_true", help="Overwrite the source blob and re-split parts.")
    parser.add_argument("--timeout", type=float, default=120.0, help="HTTP download timeout in seconds.")
    return parser


def _build_document(args: argparse.Namespace) -> tuple[SourceDocument, Optional[str]]:
    """Resolve the document to ingest from CLI flags / env, defaulting to AIR 1995.

    Raises ValueError for an invalid doc id, or when a non-default source is
    given without an explicit doc id (it would be mislabelled as AIR 1995).
    """
    source_url = args.source_url or os.getenv("SOURCE_URL")
    local_file = args.file or os.getenv("SOURCE_FILE")
    doc_id = args.doc_id or os.getenv("DOC_ID")
    year_env = os.getenv("DOC_YEAR")
    year = args.year if args.year is not None else (int(year_env) if year_env else None)

    custom_source = bool(local_file) or (source_url not in (None, "", DEFAULT_SOURCE_URL))
    if not doc_id:
        if custom_source:
            raise ValueError("--doc-id (DOC_ID) is required when ingesting a document other than AIR 1995")
        doc_id = AIR_EVALUATION_1995.doc_id
    if not DOC_ID_PATTERN.match(doc_id):
        raise ValueError(f"Invalid doc id {doc_id!r}: use lowercase letters, digits and hyphens")
    if local_file and not os.path.isfile(local_file):
        raise ValueError(f"--file not found: {local_file}")

    is_air = doc_id == AIR_EVALUATION_1995.doc_id
    if local_file and not source_url:
        source_url = f"file://{os.path.basename(local_file)}"
    doc = SourceDocument(
        doc_id=doc_id,
        title=args.title or os.getenv("DOC_TITLE") or (AIR_EVALUATION_1995.title if is_air else doc_id),
        source_url=source_url or DEFAULT_SOURCE_URL,
        publisher=args.publisher or os.getenv("DOC_PUBLISHER") or (AIR_EVALUATION_1995.publisher if is_air else ""),
        year=year if year is not None else (AIR_EVALUATION_1995.year if is_air else None),
    )
    return doc, local_file


def _resolve_blob_name(args: argparse.Namespace, doc: SourceDocument) -> str:
    """--blob-name wins; BLOB_NAME env is only honoured for the default AIR document.

    Older .env files set BLOB_NAME=source/air-evaluation-1995.pdf; applying it to
    another document would overwrite the AIR source blob.
    """
    default = f"{SOURCE_PREFIX}/{doc.doc_id}.pdf"
    if args.blob_name:
        return args.blob_name
    env_name = os.getenv("BLOB_NAME")
    if env_name and env_name != default:
        if doc.doc_id == AIR_EVALUATION_1995.doc_id:
            return env_name
        LOGGER.warning("Ignoring BLOB_NAME=%s for doc %s; using %s", env_name, doc.doc_id, default)
    return default


def _build_config(args: argparse.Namespace, doc: SourceDocument, local_file: Optional[str]) -> AppConfig:
    return AppConfig(
        source_url=doc.source_url,
        storage_account_url=args.account_url or os.getenv("AZURE_STORAGE_ACCOUNT_URL") or DEFAULT_ACCOUNT_URL,
        container=args.container or os.getenv("BLOB_CONTAINER") or DEFAULT_CONTAINER,
        blob_name=_resolve_blob_name(args, doc),
        force=args.force or os.getenv("FORCE", "").lower() in {"1", "true", "yes"},
        timeout_seconds=args.timeout,
        local_file=local_file,
    )


async def run(config: AppConfig, doc: SourceDocument) -> int:
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url=config.storage_account_url, credential=credential)
    LOGGER.info("Using DefaultAzureCredential for %s", config.storage_account_url)
    LOGGER.info("Document: %s (%s) -> %s", doc.doc_id, doc.title, config.blob_name)

    try:
        workflow = build_workflow(blob_service_client, config)
        result = await workflow.run(doc)
        outputs = result.get_outputs()
        outcomes: list[ExtractOutcome] = [o for o in outputs if isinstance(o, ExtractOutcome)]
        splits: list[SplitOutcome] = [o for o in outputs if isinstance(o, SplitOutcome)]

        print("Summary:")
        for o in outcomes:
            print(f"  [extract_source] {o.doc_id}: {o.status}")
            print(f"    blob:    {o.blob_url}")
            print(f"    size:    {o.size_bytes:,} bytes")
            if o.sha256:
                print(f"    sha256:  {o.sha256}")
            print(f"    elapsed: {o.elapsed_seconds:.1f}s")
            if o.message:
                print(f"    note:    {o.message}")
        for s in splits:
            print(f"  [split_parts] {s.doc_id}: {s.status} ({len(s.parts)} parts, {s.total_pages} pages)")
            if s.parts:
                print(f"    first:   {s.parts[0].blob_name}")
                print(f"    last:    {s.parts[-1].blob_name}")
            if s.status == "uploaded":
                print(f"    pages without extractable text: {len(s.empty_text_pages)}")
                if s.empty_text_pages:
                    shown = ", ".join(map(str, s.empty_text_pages[:20]))
                    more = " ..." if len(s.empty_text_pages) > 20 else ""
                    print(f"      {shown}{more}  (scanned? these will index empty without OCR)")
            if s.message:
                print(f"    note:    {s.message}")
        failed = any(o.status == "failed" for o in outcomes) or any(s.status == "failed" for s in splits)
        missing_split = any(o.status != "failed" for o in outcomes) and not splits
        return 1 if failed or missing_split or not outcomes else 0
    finally:
        await blob_service_client.close()
        await credential.close()


async def main_async() -> int:
    load_dotenv(override=False)
    args = _create_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logging.getLogger("azure").setLevel(logging.WARNING)
    try:
        doc, local_file = _build_document(args)
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 2
    try:
        return await run(_build_config(args, doc, local_file), doc)
    except Exception:  # noqa: BLE001
        LOGGER.exception("Fatal error")
        return 3


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
