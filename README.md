# Genie Trusted Asset Copilot

A tool that automatically improves your Databricks Genie spaces by learning from past conversations.

## What Does This Tool Do?

When your team asks questions in a Genie space, Genie generates SQL queries to answer them. Some of these queries are sophisticated and valuable—they represent proven ways to answer common business questions.

**Genie Trusted Asset Copilot** finds these valuable queries and turns them into "trusted assets" so that:

- **Future questions get faster, verified answers** — When someone asks a similar question, Genie can use the proven query instead of generating a new one
- **Answers are marked as "Trusted"** — Users see a badge indicating the response comes from a verified source
- **Complex logic becomes reusable** — Sophisticated queries are saved as functions that can be reused across your organization

## Before You Begin

### What You'll Need

1. **A Genie Space** — The space you want to improve (you'll need the Space ID)
2. **A SQL Warehouse** — Required for creating reusable functions (you'll need the Warehouse ID)
3. **A Unity Catalog location** — Where to store the reusable functions (catalog and schema names)
4. **Proper permissions** — See the Permissions section below

### How to Find Your IDs

#### Finding Your Genie Space ID

1. Open your Genie space in Databricks
2. Look at the URL in your browser — it will look like:
   ```
   https://your-workspace.databricks.com/genie/spaces/01f0c482e842191587af6a40ad4044d8
   ```
3. The Space ID is the last part: `01f0c482e842191587af6a40ad4044d8`

#### Finding Your SQL Warehouse ID

1. In Databricks, go to **SQL Warehouses** (in the left sidebar)
2. Click on your warehouse
3. Look at the URL — it will contain the warehouse ID, or
4. Click the **Connection details** tab to find the ID

### Required Permissions

| Permission | Why It's Needed |
|------------|-----------------|
| **CAN MANAGE** on the Genie space | To add trusted assets to your space |
| **CAN USE** on the SQL warehouse | To run queries and create functions |
| **CREATE** on the target schema | To create reusable functions |

If you're unsure about your permissions, contact your Databricks workspace administrator.

---

## Installation

Open a terminal and navigate to this project folder, then run:

```bash
uv sync
```

This installs all required components.

---

## Getting Started

### Step 1: Preview Changes (Recommended First Step)

Before making any changes, run the tool in "dry run" mode to see what it would do:

```bash
genie-trusted-asset-copilot \
  --space-id YOUR_SPACE_ID \
  --catalog YOUR_CATALOG \
  --schema YOUR_SCHEMA \
  --dry-run
```

**Replace the placeholders:**
- `YOUR_SPACE_ID` — Your Genie space ID (see "Finding Your Genie Space ID" above)
- `YOUR_CATALOG` — The catalog where functions will be stored (e.g., `main` or `analytics`)
- `YOUR_SCHEMA` — The schema within that catalog (e.g., `genie_functions`)

The tool will show you:
- How many conversations it found
- How many queries are complex enough to become trusted assets
- What changes it *would* make (without actually making them)

### Step 2: Run for Real

Once you're comfortable with what the tool will do, remove `--dry-run` and add your warehouse ID:

```bash
genie-trusted-asset-copilot \
  --space-id YOUR_SPACE_ID \
  --catalog YOUR_CATALOG \
  --schema YOUR_SCHEMA \
  --warehouse-id YOUR_WAREHOUSE_ID
```

The tool will:
1. Read through your Genie conversations
2. Identify complex, valuable queries
3. Add them as trusted assets to your Genie space
4. Create reusable functions in Unity Catalog
5. Register those functions with your Genie space

---

## Common Options

### Limit How Many Conversations to Process

If you have many conversations and want to start small:

```bash
genie-trusted-asset-copilot \
  --space-id YOUR_SPACE_ID \
  --catalog YOUR_CATALOG \
  --schema YOUR_SCHEMA \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --max-conversations 20
```

### Include More Queries

By default, only highly complex queries become trusted assets. To include moderately complex queries too:

```bash
genie-trusted-asset-copilot \
  --space-id YOUR_SPACE_ID \
  --catalog YOUR_CATALOG \
  --schema YOUR_SCHEMA \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --threshold moderate
```

### Replace Existing Assets

If you want to update assets that already exist (instead of skipping them):

```bash
genie-trusted-asset-copilot \
  --space-id YOUR_SPACE_ID \
  --catalog YOUR_CATALOG \
  --schema YOUR_SCHEMA \
  --warehouse-id YOUR_WAREHOUSE_ID \
  --force
```

### Choose What to Create

You can control exactly what the tool creates:

| Option | What It Does |
|--------|--------------|
| `--no-sql-instructions` | Don't add SQL examples to the Genie space |
| `--no-uc-functions` | Don't create reusable functions |
| `--no-register-functions` | Create functions but don't add them to the Genie space |

**Example:** Only add SQL instructions (no functions):

```bash
genie-trusted-asset-copilot \
  --space-id YOUR_SPACE_ID \
  --catalog YOUR_CATALOG \
  --schema YOUR_SCHEMA \
  --no-uc-functions
```

---

## Understanding the Output

When you run the tool, you'll see progress messages like this:

```
Starting Genie Trusted Asset Copilot
Space ID: 01f0c482e842191587af6a40ad4044d8
Target catalog.schema: analytics.genie_functions

Step 1: Reading conversations and extracting SQL queries...
Extracted 45 SQL queries

Step 2: Analyzing SQL complexity...
Found 12 complex queries

Step 3: Creating trusted assets and UC functions...
Added 12 trusted assets to Genie space
Created 12/12 UC functions
Registered 12 functions with Genie room

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

### What the Numbers Mean

| Metric | Meaning |
|--------|---------|
| **Queries extracted** | Total number of successful SQL queries found in conversations |
| **Complex queries found** | Queries sophisticated enough to become trusted assets |
| **Trusted assets created** | SQL examples added to your Genie space |
| **UC functions created** | Reusable functions created in Unity Catalog |
| **UC functions registered** | Functions connected to your Genie space |

---

## Troubleshooting

### "Permission denied" errors

Make sure you have the required permissions listed in the "Required Permissions" section. Contact your Databricks administrator if you need access.

### "No SQL queries found"

This happens when:
- The Genie space has no conversations yet
- All conversations had errors (only successful queries are processed)

Try using a Genie space with more conversation history.

### "No complex queries found"

The queries in your conversations may be too simple. Try:
- Using `--threshold moderate` to include moderately complex queries
- Processing more conversations with `--max-conversations 100`

### Functions aren't appearing in Genie

Make sure:
1. You included `--warehouse-id` in your command
2. You didn't use `--no-register-functions`
3. You have CREATE permission on the target schema

---

## All Options Reference

| Option | Description | Default |
|--------|-------------|---------|
| `--space-id` | Your Genie space ID (required) | — |
| `--catalog` | Where to store functions (required) | — |
| `--schema` | Schema within the catalog (required) | — |
| `--warehouse-id` | SQL warehouse for creating functions | — |
| `--max-conversations` | Limit conversations to process | All |
| `--threshold` | Complexity level: `simple`, `moderate`, or `complex` | `complex` |
| `--dry-run` | Preview without making changes | Off |
| `--force` | Replace existing assets | Off |
| `--sql-instructions` / `--no-sql-instructions` | Create SQL examples | On |
| `--uc-functions` / `--no-uc-functions` | Create functions | On |
| `--register-functions` / `--no-register-functions` | Register functions with Genie | On |
| `--verbose` | Show detailed logging | Off |

---

## Getting Help

If you encounter issues:

1. Run with `--verbose` to see detailed information:
   ```bash
   genie-trusted-asset-copilot --space-id X --catalog Y --schema Z --verbose
   ```

2. Try `--dry-run` first to preview changes without risk

3. Contact your Databricks administrator for permission issues

---

## License

MIT
