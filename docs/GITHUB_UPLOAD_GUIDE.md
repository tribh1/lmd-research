# GitHub Upload Guide

This guide describes how to publish this prototype as a GitHub repository.

## 1. Prepare repository locally

```bash
cd lakehouse-metadata-driven-kappa-lakehouse
cp .env.example .env
```

Review `.env` before running. Do not commit `.env`.

## 2. Initialize Git

```bash
git init
git add .
git status
git commit -m "Initial strict layered metadata-driven Kappa Lakehouse prototype"
```

## 3. Create GitHub repository

Create an empty repository on GitHub, for example:

```text
metadata-driven-kappa-lakehouse
```

Do not initialize it with a README, because this package already contains one.

## 4. Add remote and push

```bash
git branch -M main
git remote add origin https://github.com/<your-org-or-user>/metadata-driven-kappa-lakehouse.git
git push -u origin main
```

## 5. Recommended GitHub repository settings

- Add repository description:
  `Strict layered metadata-driven Kappa Lakehouse prototype with embedded metadata, governance, OpenMetadata and Airflow orchestration.`
- Add topics:
  `lakehouse`, `kappa-architecture`, `metadata-driven`, `spark`, `iceberg`, `kafka`, `debezium`, `openmetadata`, `airflow`, `data-governance`.
- Keep `.env` excluded from Git.
- Decide a license before making the repository public.

## 6. Optional GitHub CLI commands

If GitHub CLI is installed and authenticated:

```bash
gh repo create metadata-driven-kappa-lakehouse --private --source=. --remote=origin --push
```

For public release:

```bash
gh repo edit metadata-driven-kappa-lakehouse --visibility public
```

## 7. Files intentionally ignored

Runtime outputs are ignored by `.gitignore`, including:

```text
.env
results/**/*.json
results/**/*.csv
results/dashboard/*.md
data/generated/
__pycache__/
*.pyc
spark-warehouse/
warehouse/
```

Placeholder `.gitkeep` files preserve folder structure.
