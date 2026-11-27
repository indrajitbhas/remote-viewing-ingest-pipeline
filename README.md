# rv-ingest-pipeline

Remote-viewing document ingestion pipeline built with **Microsoft Agent Framework** workflows (same pattern as `uap-pdf-content-understanding`).

## Node 1: `extract_source`
Downloads the **AIR evaluation (1995)** PDF ("An Evaluation of Remote Viewing: Research and Applications", 183 pages) from the National Security Archive and uploads it to:

`https://uapstorage1.blob.core.windows.net/remote-viewing-docs-air-evaluation-1995/source/air-evaluation-1995.pdf`

Blob metadata records `source_url`, `sha256`, `retrieved_at`, `title`, `publisher`, `year`. Re-runs are skipped if the blob exists (use `--force` to overwrite).

## Auth
`DefaultAzureCredential` (run `az login` locally). Your identity needs **Storage Blob Data Contributor** on `uapstorage1`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
az login
python main.py            # add --force to overwrite
```

## Azure AI Search (indexing)
The PDF has a native text layer (Word-generated, 0 images), so AI Search reads it directly: no OCR or Content Understanding.
Chunking, entity/key-phrase extraction and embeddings run in the skillset. See [`azure-search/README.md`](azure-search/README.md) for the fields and required roles.

```bash
# fill the Azure AI Search section of .env first
python deploy_search.py --dry-run   # render definitions, no Azure calls
python deploy_search.py --run       # deploy, then reset + run the indexer
```

## Next nodes
Chain them in `build_workflow()` with `.add_edge(extract_source, <next_node>)`, e.g. Content Understanding analysis → cleanup → chunking → AI Search indexing.
