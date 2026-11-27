# rv-ingest-pipeline

Remote-viewing document ingestion pipeline built with **Microsoft Agent Framework** workflows (same pattern as `uap-pdf-content-understanding`).

## Node 1: `extract_source`
Downloads the **AIR evaluation (1995)** PDF ("An Evaluation of Remote Viewing: Research and Applications", 183 pages) from the National Security Archive and uploads it to:

`https://uapstorage1.blob.core.windows.net/remote-viewing-docs-air-evaluation-1995/source/air-evaluation-1995.pdf`

Blob metadata records `source_url`, `sha256`, `retrieved_at`, `title`, `publisher`, `year`. Re-runs are skipped if the blob exists (use `--force` to overwrite).

## Node 2: `split_parts`
Reads the stored PDF back from Blob Storage and splits it at page boundaries into parts of at most 200,000 extracted characters (`--max-part-chars` / `MAX_PART_CHARS`). The search indexer truncates each blob at 262,144 characters, and the full report has ~384k, so a single blob would lose everything after page ~119.

AIR 1995 becomes 2 parts:

| Blob | Pages | Chars |
|---|---|---|
| `parts/air-evaluation-1995/air-evaluation-1995-p001-090.pdf` | 1-90 | 197,837 |
| `parts/air-evaluation-1995/air-evaluation-1995-p091-183.pdf` | 91-183 | 185,879 |

Each part carries `page_from`, `page_to`, `page_range` and the source metadata. The search data source reads `parts/` only. Skipped if parts exist; `--force` rewrites them.

## Auth
`DefaultAzureCredential` (run `az login` locally). Your identity needs **Storage Blob Data Contributor** on `uapstorage1`.

## Run
```bash
python -m venv .venv && source .venv/Scripts/activate
pip install -r requirements.txt
cp .env.example .env
az login
python main.py            # node 1 + node 2; add --force to overwrite
```

## Azure AI Search (indexing)
The PDF has a native text layer (Word-generated, 0 images), so AI Search reads it directly: no OCR or Content Understanding.
Chunking, entity/key-phrase extraction and embeddings run in the skillset. See [`azure-search/README.md`](azure-search/README.md) for the fields and required roles.

```bash
# fill the Azure AI Search section of .env first
python deploy_search.py --dry-run   # render definitions, no Azure calls
python deploy_search.py --run       # deploy, then reset + run the indexer
python deploy_search.py --status    # wait for the indexer run, print items/errors/chunk count
```

## Next nodes
Chain them in `build_workflow()` with `.add_edge(split_parts, <next_node>)`.
