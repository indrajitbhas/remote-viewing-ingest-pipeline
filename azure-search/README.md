# Azure AI Search definitions

Deployed by `../deploy_search.py` (keyless, `DefaultAzureCredential`). `__PLACEHOLDER__` tokens are filled from `.env`.

| File | Object | What it does |
|---|---|---|
| `datasource.json` | `remote-viewing-blob-ds` | Blob container `remote-viewing-docs-air-evaluation-1995`, prefix `parts/` (page-range parts from node 2), connected with the search service's managed identity (`ResourceId=...`). |
| `index.json` | `remote-viewing-index` | One record per **chunk**: text, 1536-dim vector, document fields, per-chunk entities and key phrases. HNSW + Azure OpenAI vectorizer, semantic config `rv-semantic`. |
| `skillset.json` | `remote-viewing-skillset` | Text Split (2,000 chars, 400 overlap) → Entity Recognition v3 (Person/Organization/Location) → Key Phrases → Azure OpenAI embeddings. Index projections write chunks only (`skipIndexingParentDocuments`). |
| `indexer.json` | `remote-viewing-indexer` | Native PDF text extraction (`imageAction: none`, no OCR, no Content Understanding). |

## Filterable / facetable fields

| Field | Level | Source | Use |
|---|---|---|---|
| `persons`, `organizations`, `locations` | chunk | Entity Recognition | "passages mentioning Puthoff / SRI / CIA" |
| `key_phrases` | chunk | Key Phrase Extraction | topic facets, semantic keywords |
| `page_number` | page | blob metadata written by node 2 (`Edm.Int32`, filterable, sortable) | exact page citation, e.g. `$filter=page_number ge 40 and page_number le 60`, `$orderby=page_number` |
| `doc_id`, `title`, `publisher`, `doc_year` | document | blob metadata written by node 1 | constant today (one document); useful once more documents are added |

Example: `search=accuracy of operational tasking&$filter=organizations/any(o: o eq 'SRI International')&facet=persons,count:20`

Facet values are the raw entity text, so expect variants ("CIA" vs "Central Intelligence Agency"). Look at the facets after the first run before building filter UI on them.

## Required roles

| Who | Role | On |
|---|---|---|
| You (`az login`) | Search Service Contributor | search service |
| Search service managed identity | Storage Blob Data Reader | `uapstorage1` |
| Search service managed identity | Cognitive Services OpenAI User | Azure OpenAI / Foundry resource |
| Search service managed identity | Cognitive Services User | AI Services resource (billing for entities/key phrases) |

Search service: enable the system-assigned identity, and set API access control to **RBAC** or **Both**.

## Not included
- **`AZURE_OPENAI_ENDPOINT`** must be the resource root (`https://<name>.openai.azure.com`). `deploy_search.py` strips any path such as `/openai/v1`, which otherwise causes a 404 in the embedding skill.
- **Chapter/section field**: not reliably produced by built-in skills.
- `dimensions` is fixed at 1536 (`text-embedding-3-small`). Change it in both `index.json` and `skillset.json` if you use another model.
