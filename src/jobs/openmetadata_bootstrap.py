"""Register the lakehouse catalog in OpenMetadata from pipeline_config.yaml.

This job realizes the metadata-as-control-plane bootstrap (thesis Section 3.3):
service, database, Medallion schemas, tables with column-level PII tags, the
business glossary, ownership, and static design-time lineage. Run it once after
the OpenMetadata container is healthy and before the experiments:

    python -m src.jobs.openmetadata_bootstrap --config metadata/pipeline_config.yaml
"""
from __future__ import annotations

import argparse
from typing import Dict, List, Optional
import yaml

from src.common.metadata_client import MetadataClient

MEDALLION_SCHEMAS = {
    "source": "PostgreSQL transactional source tables (registered for lineage completeness)",
    "raw": "Raw layer: immutable landing zone preserving source fidelity",
    "work": "Work layer: governance preparation boundary (technical standardization)",
    "silver": "Silver layer: conformed, governance-validated single source of truth",
    "gold": "Gold layer: business-level KPI aggregates",
    "mart": "Data Mart layer: domain-specific serving datasets",
    "audit": "Append-only audit evidence tables",
    "quarantine": "Quality-failed records pending investigation",
}

PII_TAGS = {
    "Name": "Person full name",
    "Email": "Email address",
    "Phone": "Telephone number",
    "Financial": "Payment card or account identifier",
    "Address": "Postal address",
}

GOLD_TABLES = {
    "gold": {
        "daily_revenue_kpi": {
            "description": "Daily gross revenue and paid amount per channel (KPI: Daily Revenue).",
            "columns": [
                {"name": "business_date", "dataType": "DATE"},
                {"name": "channel", "dataType": "TEXT"},
                {"name": "order_count", "dataType": "BIGINT"},
                {"name": "gross_revenue", "dataType": "DECIMAL"},
                {"name": "paid_amount", "dataType": "DECIMAL"},
            ],
            "glossary_terms": ["KPI", "Revenue"],
        },
        "product_sales_kpi": {
            "description": "Quantity sold and net sales per product and category (KPI: Product Sales).",
            "columns": [
                {"name": "category", "dataType": "TEXT"},
                {"name": "product_id", "dataType": "BIGINT"},
                {"name": "product_name", "dataType": "TEXT"},
                {"name": "qty_sold", "dataType": "BIGINT"},
                {"name": "net_sales", "dataType": "DECIMAL"},
            ],
            "glossary_terms": ["KPI", "Product"],
        },
    },
    "mart": {
        "sales_dashboard": {
            "description": "Consumption-oriented sales dashboard mart joining revenue KPIs per channel and date.",
            "columns": [
                {"name": "business_date", "dataType": "DATE"},
                {"name": "channel", "dataType": "TEXT"},
                {"name": "order_count", "dataType": "BIGINT"},
                {"name": "gross_revenue", "dataType": "DECIMAL"},
                {"name": "paid_amount", "dataType": "DECIMAL"},
                {"name": "revenue_gap", "dataType": "DECIMAL"},
            ],
            "glossary_terms": ["Sales Dashboard", "Data Product"],
        },
    },
}


def om_columns(spec: Dict) -> List[Dict]:
    cols = []
    for c in spec.get("columns", []):
        col = {"name": c["name"], "dataType": c["dataType"]}
        if c.get("pii"):
            col["tags"] = [{"tagFQN": c["pii"]}]
        cols.append(col)
    return cols


def glossary_tags(terms: List[str], glossary: str) -> List[Dict]:
    return [{"tagFQN": f"{glossary}.{t.replace(' ', '_')}", "source": "Glossary"} for t in terms]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--openmetadata-url", default=None)
    args = ap.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    om_url = args.openmetadata_url or cfg["environment"]["openmetadata"]["url"]
    om = MetadataClient(om_url)
    if not om.available():
        raise SystemExit(f"OpenMetadata is not reachable at {om_url}. Start the openmetadata service first.")

    defaults = cfg.get("catalog_defaults", {})
    glossary = "EnterpriseSales"
    owner_name = defaults.get("owner", "data-platform-team")

    team = om.ensure_team(owner_name, "Data platform stewardship team")
    owner: Optional[Dict] = {"id": team["id"], "type": "team"} if team and team.get("id") else None

    om.ensure_classification("PII", "Personally identifiable information taxonomy (thesis Section 3.4.2)")
    for tag, desc in PII_TAGS.items():
        om.ensure_tag("PII", tag, desc)

    om.ensure_glossary(glossary, "Business glossary for the enterprise sales domain")
    all_terms = set()
    for spec in cfg["tables"].values():
        all_terms.update(spec.get("glossary_terms", []))
    for tables in GOLD_TABLES.values():
        for spec in tables.values():
            all_terms.update(spec.get("glossary_terms", []))
    for term in sorted(all_terms):
        om.ensure_glossary_term(glossary, term.replace(" ", "_"), term)

    om.ensure_service()
    om.ensure_database()
    for schema, desc in MEDALLION_SCHEMAS.items():
        om.ensure_schema(schema, desc)

    registered = 0
    for table, spec in cfg["tables"].items():
        cols = om_columns(spec)
        desc = spec.get("description", "")
        tags = glossary_tags(spec.get("glossary_terms", []), glossary)
        src_name = spec["source_table"].split(".")[-1]
        for schema, schema_desc in [
            ("source", f"PostgreSQL source table {spec['source_table']}"),
            ("raw", f"Raw copy of {spec['source_table']} (immutable, unmasked)"),
            ("work", f"Technically standardized {table} before governance validation"),
            ("silver", f"Conformed {table}: quality-validated and PII-masked"),
        ]:
            name = src_name if schema == "source" else table
            if om.ensure_table(schema, name, cols, f"{desc} {schema_desc}".strip(), owner, tags):
                registered += 1

    for schema, tables in GOLD_TABLES.items():
        for table, spec in tables.items():
            if om.ensure_table(schema, table, om_columns(spec), spec["description"],
                               owner, glossary_tags(spec["glossary_terms"], glossary)):
                registered += 1

    # Static design-time lineage: source -> raw -> work -> silver per table,
    # then the gold/mart edges declared in the config lineage section.
    edges = 0
    for table, spec in cfg["tables"].items():
        chain = [spec["source_table"], f"lakehouse.raw.{table}",
                 f"lakehouse.work.{table}", f"lakehouse.silver.{table}"]
        for up, down in zip(chain, chain[1:]):
            if om.emit_lineage(up, down, f"design-lineage-{table}", "bootstrap"):
                edges += 1
    for node, spec in cfg.get("lineage", {}).items():
        for up in spec.get("upstream", []):
            if om.emit_lineage(f"lakehouse.{up}", f"lakehouse.{node}", "design-lineage", "bootstrap"):
                edges += 1

    print({"registered_tables": registered, "lineage_edges": edges, "openmetadata": om_url})


if __name__ == "__main__":
    main()
