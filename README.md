# rv-ingest-pipeline

Remote-viewing document ingestion pipeline built with **Microsoft Agent Framework** workflows (same pattern as `uap-pdf-content-understanding`).

## Node 1: `extract_source`
Downloads the **AIR evaluation (1995)** PDF ("An Evaluation of Remote Viewing: Research and Applications", 183 pages) from the National Security Archive and uploads it to:

`https://uapstorage1.blob.core.windows.net/remote-viewing-docs-air-evaluation-1995/source/air-evaluation-1995.pdf`

Blob metadata records `source_url`, `sha256`, `retrieved_at`, `title`, `publisher`, `year`. Re-runs are skipped if the blob exists (use `--force` to overwrite).

## Node 2: `split_parts`
Reads the stored PDF back from Blob Storage and splits it into **one PDF per page**, so search results can cite the exact page. AIR 1995 becomes 183 blobs:

`parts/air-evaluation-1995/air-evaluation-1995-p001.pdf` … `air-evaluation-1995-p183.pdf`

Each page blob carries a `page_number` metadata value plus the source metadata. It lands in the index as the integer `page_number` field (filterable, sortable), so every chunk can be cited as "AIR 1995, p. 42". A single page is far below the indexer's 262,144-character extraction limit. The search data source reads `parts/` only. Skipped if parts exist; `--force` deletes and rewrites them (use it once to replace the old page-range parts).

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
