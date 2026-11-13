# ADR 0002: Single SDP pipeline and metadata-driven Bronze ingestion

## Status
Accepted — 2026-11-12

## Context
The platform needs Bronze, Silver and Gold layers built with Lakeflow Spark
Declarative Pipelines (SDP). Two design questions came up while building Bronze:

1. Should each layer (or each source) have its own pipeline, or should one
   pipeline contain all layers?
2. Should each Bronze table be written as its own hand-coded function, or
   generated from configuration?

Constraints:
- Databricks Free Edition allows only one active pipeline per pipeline type.
- There are six landing sources today (customers_cdc, products_cdc, stores,
  orders, payments, shipments) and more are planned (e.g. inventory snapshots).
- All sources share the same format (JSON lines) and the same ingestion rules.

## Decision 1: One pipeline for all layers
A single pipeline, `shopstream_pipeline`, holds Bronze, Silver and Gold.
Its default schema is `bronze`; Silver and Gold tables will be published to
their own schemas using fully qualified names.

### Alternatives considered
- **One pipeline per layer**: independent schedules, but needs orchestration
  between pipelines, and breaks the Free Edition limit.
- **One pipeline per source**: maximum isolation, but many pipelines to deploy
  and monitor, and no single dependency graph.

### Consequences
- (+) SDP sees the full dependency graph: it orders the work automatically
  and shows end-to-end lineage in one view.
- (+) One resource to deploy, run and monitor per environment.
- (+) Fits within the Free Edition pipeline limit.
- (−) Larger blast radius: a broken table definition can fail the whole update.
- (−) All layers share one trigger; one layer can't be refreshed on a different
  schedule than another.
- (−) As the pipeline grows, a full refresh becomes more expensive, so refreshes
  should be scoped to specific tables.

## Decision 2: Metadata-driven Bronze ingestion
Bronze tables are generated from one configuration dictionary
(`BRONZE_SOURCES`) by a single factory function (`create_bronze_table`).
Every table gets the same Auto Loader settings and the same ingest metadata
(`_source_file`, `_file_modified_ts`, `_batch_id`, `_ingest_ts`), plus a
warn-only expectation on `_rescued_data`.

### Alternatives considered
- **One hand-written function per table**: explicit and easy to read, but
  copy-pasted code drifts over time and every new source means a new function.

### Consequences
- (+) Adding a source is a one-line change.
- (+) Consistent ingestion rules and metadata across all Bronze tables.
- (−) Less flexible when one source needs special handling (e.g. a CSV source
  or custom schema hints); the config will need per-source options.
- (−) Slightly harder to read and debug for newcomers. The factory function is
  required to avoid Python's late-binding problem when defining tables in a loop.