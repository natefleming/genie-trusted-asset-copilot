# Genie Trusted Asset Copilot

Automatically create Genie trusted assets (SQL example instructions) and Unity Catalog functions from complex SQL queries in existing Genie conversations.

## Overview

This tool analyzes Genie conversations to:
1. Extract SQL queries from **successful** message responses only (status: COMPLETED)
2. Evaluate query complexity using ChatDatabricks (with `databricks-claude-sonnet-4`)
3. Create trusted assets for complex queries in the Genie space
4. Optionally create Unity Catalog functions for reusable SQL

Only messages with successful execution status are considered. Failed queries, errors, and incomplete responses are automatically filtered out.

## Installation

```bash
# Using uv
uv sync

# Or using pip
pip install -e .
```

## Prerequisites

- Access to a Databricks workspace with Genie spaces
- Databricks authentication configured (via environment variables or profile)
- CAN USE permission on a SQL warehouse
- CAN MANAGE permission on the Genie space (to update trusted assets)
- CREATE permission on the target Unity Catalog schema (for UC functions)

## Usage

### Basic Usage (Dry Run)

Preview what changes would be made without applying them:

```bash
genie-trusted-asset-copilot \
  --space-id <genie_space_id> \
  --catalog <catalog_name> \
  --schema <schema_name> \
  --dry-run
```

### Create Trusted Assets and UC Functions

```bash
genie-trusted-asset-copilot \
  --space-id <genie_space_id> \
  --catalog <catalog_name> \
  --schema <schema_name> \
  --warehouse-id <sql_warehouse_id>
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--space-id` | Genie space ID to process (required) | - |
| `--catalog` | Unity Catalog name for functions (required) | - |
| `--schema` | Schema name within the catalog (required) | - |
| `--warehouse-id` | SQL warehouse ID for creating UC functions | - |
| `--max-conversations` | Limit number of conversations to process | All |
| `--include-all-users` | Include conversations from all users | False |
| `--model` | Databricks model for complexity analysis | `databricks-claude-sonnet-4` |
| `--threshold` | Minimum complexity threshold (`simple`, `moderate`, `complex`) | `complex` |
| `--dry-run` | Preview changes without applying | False |
| `--force`, `-f` | Replace existing assets instead of skipping | False |
| `--sql-instructions` / `--no-sql-instructions` | Create SQL example instructions in Genie room | Enabled |
| `--uc-functions` / `--no-uc-functions` | Create UC functions in Unity Catalog | Enabled |
| `--register-functions` / `--no-register-functions` | Register UC functions with Genie room | Enabled |
| `--verbose`, `-v` | Enable debug logging | False |

### Selective Creation Examples

```bash
# Only create SQL instructions (no UC functions)
genie-trusted-asset-copilot --space-id X --catalog Y --schema Z --no-uc-functions

# Only create UC functions in UC (don't register with Genie room)
genie-trusted-asset-copilot --space-id X --catalog Y --schema Z \
  --warehouse-id W --no-sql-instructions --no-register-functions

# Create UC functions and register with Genie (no SQL instructions)
genie-trusted-asset-copilot --space-id X --catalog Y --schema Z \
  --warehouse-id W --no-sql-instructions

# Force replace existing assets and functions
genie-trusted-asset-copilot --space-id X --catalog Y --schema Z --warehouse-id W --force
```

## Complexity Analysis

The tool uses an LLM to classify SQL query complexity based on:

- **SIMPLE**: Basic SELECT with simple WHERE clauses, no JOINs or subqueries
- **MODERATE**: Contains JOINs, GROUP BY, or simple aggregations
- **COMPLEX**: Contains multiple JOINs, subqueries, CTEs, window functions, or complex aggregations

By default, only COMPLEX queries are promoted to trusted assets. Use `--threshold moderate` to include moderate complexity queries as well.

## Output

### Trusted Assets

Complex queries are added as SQL example instructions in the Genie space configuration. These appear in the space's instructions and are used to provide "Trusted" responses when users ask similar questions.

### Unity Catalog Functions

For each complex query, a table-valued function is created in the specified catalog/schema:

```sql
CREATE OR REPLACE FUNCTION catalog.schema.genie_<query_name>()
RETURNS TABLE
LANGUAGE SQL
COMMENT 'Auto-generated from Genie conversation...'
RETURN (SELECT ...)
```

## Example Output

```
2026-01-28 10:30:00 | INFO     | Starting Genie Trusted Asset Copilot
2026-01-28 10:30:00 | INFO     | Space ID: abc123
2026-01-28 10:30:00 | INFO     | Target catalog.schema: analytics.genie_functions
2026-01-28 10:30:01 | INFO     | Step 1: Reading conversations and extracting SQL queries...
2026-01-28 10:30:05 | INFO     | Extracted 45 SQL queries
2026-01-28 10:30:05 | INFO     | Step 2: Analyzing SQL complexity...
2026-01-28 10:30:30 | INFO     | Found 12 complex queries
2026-01-28 10:30:30 | INFO     | Step 3: Creating trusted assets and UC functions...
2026-01-28 10:30:35 | SUCCESS  | Added 12 trusted assets to Genie space
2026-01-28 10:30:45 | INFO     | Created 12/12 UC functions
2026-01-28 10:30:50 | SUCCESS  | Registered 12 functions with Genie room
============================================================
Processing Complete - Summary
============================================================
Queries extracted: 45
Complex queries found: 12
Trusted assets created: 12
UC functions created: 12
UC functions registered: 12
============================================================
```

## Development

```bash
# Install with dev dependencies
uv sync

# Run linting
uv run ruff check .

# Format code
uv run ruff format .
```

## License

MIT
