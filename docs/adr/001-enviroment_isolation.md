# ADR 0001: Environment isolation via Unity Catalog catalogs

## Status
Accepted — 2026-11-10

## Context
We need isolated dev, staging and prod environments. Databricks Free Edition
allows only one workspace and one metastore.

## Decision
- One catalog per environment: dev_shop, stg_shop, prd_shop.
- Catalogs are platform-level, created once by `sql/setup/00_create_catalogs.sql`.
- Schemas, volumes, pipelines and jobs are deployed per target with a Databricks
  Asset Bundle; the catalog name is the only thing that changes between targets.