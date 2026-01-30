# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Trusted Asset Copilot
# MAGIC
# MAGIC This notebook automatically improves your Genie space by learning from past conversations.
# MAGIC
# MAGIC **What it does:**
# MAGIC - Reads through conversations in your Genie space
# MAGIC - Identifies complex, valuable SQL queries
# MAGIC - Creates "trusted assets" so future similar questions get verified answers
# MAGIC - Optionally creates reusable functions in Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Use
# MAGIC
# MAGIC 1. Clone this repository as a Git folder in your Databricks workspace
# MAGIC 2. Fill in the parameters in the widgets at the top of the notebook
# MAGIC 3. Run all cells
# MAGIC 4. Review the results at the bottom

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Dependencies
# MAGIC
# MAGIC This cell installs required packages. Run this cell first, then continue with the rest.

# COMMAND ----------

# MAGIC %pip install --quiet -r ../requirements.txt

# COMMAND ----------

# Restart Python to pick up new packages
dbutils.library.restartPython()

# COMMAND ----------

# Add the project root to Python path so we can import the package
import sys
sys.path.insert(0, "..")

# COMMAND ----------

# Import timestamp parsing utilities
import re
from datetime import datetime, timedelta, timezone

def parse_timestamp(timestamp_str: str) -> int:
    """
    Parse timestamp string into Unix milliseconds.

    Supports multiple formats:
    - Relative: 7d (days), 24h (hours), 30m (minutes), 1w (weeks)
    - ISO 8601: 2026-01-15T10:30:00 or 2026-01-15T10:30:00Z
    - Date: 2026-01-15 (assumes start of day in UTC)

    Args:
        timestamp_str: The timestamp string to parse.

    Returns:
        Unix timestamp in milliseconds.

    Raises:
        ValueError: If the timestamp format is not recognized.
    """
    timestamp_str = timestamp_str.strip()

    # Try relative format first (e.g., 7d, 24h, 30m, 1w)
    relative_pattern = r"^(\d+)([dhwm])$"
    match = re.match(relative_pattern, timestamp_str, re.IGNORECASE)
    if match:
        value = int(match.group(1))
        unit = match.group(2).lower()

        now = datetime.now(timezone.utc)
        if unit == "m":
            delta = timedelta(minutes=value)
        elif unit == "h":
            delta = timedelta(hours=value)
        elif unit == "d":
            delta = timedelta(days=value)
        elif unit == "w":
            delta = timedelta(weeks=value)
        else:
            raise ValueError(f"Unknown time unit: {unit}")

        target_time = now - delta
        return int(target_time.timestamp() * 1000)

    # Try ISO 8601 format with timezone
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]:
        try:
            # Handle Z suffix explicitly
            ts_str = timestamp_str.replace("Z", "+00:00") if "Z" in timestamp_str else timestamp_str
            dt = datetime.strptime(ts_str, fmt)
            # If no timezone info, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue

    # Try simple date format (assumes start of day UTC)
    try:
        dt = datetime.strptime(timestamp_str, "%Y-%m-%d")
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        pass

    raise ValueError(
        f"Unable to parse timestamp: {timestamp_str}\n"
        f"Supported formats:\n"
        f"  - Relative: 7d, 24h, 30m, 1w\n"
        f"  - ISO 8601: 2026-01-15T10:30:00, 2026-01-15T10:30:00Z\n"
        f"  - Date: 2026-01-15"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure Parameters
# MAGIC
# MAGIC Use the widgets above to set your parameters.

# COMMAND ----------

# Create widgets for all parameters
dbutils.widgets.text("space_id", "", "1. Genie Space ID")
dbutils.widgets.text("catalog", "", "2. Unity Catalog Name")
dbutils.widgets.text("schema", "", "3. Schema Name")
dbutils.widgets.text("warehouse_id", "", "4. SQL Warehouse ID (optional)")
dbutils.widgets.text("max_conversations", "", "5. Max Conversations (optional, most recent N)")
dbutils.widgets.dropdown("complexity_threshold", "complex", ["simple", "moderate", "complex"], "6. Complexity Threshold")
dbutils.widgets.dropdown("dry_run", "No", ["Yes", "No"], "7. Dry Run (preview only)?")
dbutils.widgets.dropdown("force_replace", "No", ["Yes", "No"], "8. Force Replace Existing?")
dbutils.widgets.dropdown("create_sql_instructions", "Yes", ["Yes", "No"], "9. Create SQL Instructions?")
dbutils.widgets.dropdown("create_uc_functions", "Yes", ["Yes", "No"], "10. Create UC Functions?")
dbutils.widgets.dropdown("register_functions", "Yes", ["Yes", "No"], "11. Register Functions with Genie?")
dbutils.widgets.text("from_timestamp", "", "12. From Timestamp (e.g., 7d, 2026-01-15, empty = no filter)")
dbutils.widgets.text("to_timestamp", "", "13. To Timestamp (e.g., 2026-01-31, empty = no filter)")
dbutils.widgets.text("num_workers", "4", "14. Number of Concurrent Workers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run the Copilot
# MAGIC
# MAGIC This cell reads the parameters and runs the trusted asset creation workflow.
# MAGIC
# MAGIC **Note:** Make sure you've filled in the widget parameters above before running.

# COMMAND ----------

# Get parameter values from widgets
space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
warehouse_id = dbutils.widgets.get("warehouse_id").strip() or None
max_conversations_str = dbutils.widgets.get("max_conversations").strip()
complexity_threshold = dbutils.widgets.get("complexity_threshold")
dry_run = dbutils.widgets.get("dry_run") == "Yes"
force_replace = dbutils.widgets.get("force_replace") == "Yes"
create_sql_instructions = dbutils.widgets.get("create_sql_instructions") == "Yes"
create_uc_functions = dbutils.widgets.get("create_uc_functions") == "Yes"
register_functions = dbutils.widgets.get("register_functions") == "Yes"
from_timestamp_str = dbutils.widgets.get("from_timestamp").strip()
to_timestamp_str = dbutils.widgets.get("to_timestamp").strip()
num_workers_str = dbutils.widgets.get("num_workers").strip()

# Convert max_conversations to int if provided
max_conversations = int(max_conversations_str) if max_conversations_str else None

# Convert num_workers to int (default to 4 if invalid)
try:
    num_workers = int(num_workers_str) if num_workers_str else 4
except ValueError:
    num_workers = 4
    print(f"Warning: Invalid num_workers value '{num_workers_str}', using default: 4")

# Parse timestamp filters if provided
from_ts = None
to_ts = None

if from_timestamp_str:
    from_ts = parse_timestamp(from_timestamp_str)
    print(f"Parsed from_timestamp: {datetime.fromtimestamp(from_ts / 1000, tz=timezone.utc).isoformat()}")

if to_timestamp_str:
    to_ts = parse_timestamp(to_timestamp_str)
    print(f"Parsed to_timestamp: {datetime.fromtimestamp(to_ts / 1000, tz=timezone.utc).isoformat()}")

# Validate required parameters
if not space_id:
    raise ValueError("Please provide a Genie Space ID in widget 1")
if not catalog:
    raise ValueError("Please provide a Unity Catalog name in widget 2")
if not schema:
    raise ValueError("Please provide a Schema name in widget 3")

# Display configuration
print("=" * 60)
print("Configuration")
print("=" * 60)
print(f"Genie Space ID:          {space_id}")
print(f"Target Location:         {catalog}.{schema}")
print(f"SQL Warehouse ID:        {warehouse_id or '(not provided)'}")
print(f"Max Conversations:       {max_conversations or 'All'}")
print(f"Complexity Threshold:    {complexity_threshold}")
print(f"Dry Run:                 {dry_run}")
print(f"Force Replace:           {force_replace}")
print(f"Create SQL Instructions: {create_sql_instructions}")
print(f"Create UC Functions:     {create_uc_functions}")
print(f"Register Functions:      {register_functions}")
print(f"From Timestamp:          {from_timestamp_str or 'None (no filter)'}")
print(f"To Timestamp:            {to_timestamp_str or 'None (no filter)'}")
print(f"Concurrent Workers:      {num_workers}")
print("=" * 60)

# COMMAND ----------

# Import and run the copilot
from genie_trusted_asset_copilot.main import run
from genie_trusted_asset_copilot.logging_config import configure_logging

# Configure logging to show in notebook
configure_logging(level="INFO")

# Run the workflow
report = run(
    space_id=space_id,
    catalog=catalog,
    schema=schema,
    warehouse_id=warehouse_id,
    max_conversations=max_conversations,
    include_all_users=False,
    model="databricks-claude-sonnet-4",
    complexity_threshold=complexity_threshold,
    dry_run=dry_run,
    force=force_replace,
    create_sql_instructions=create_sql_instructions,
    create_uc_functions=create_uc_functions,
    register_uc_functions=register_functions,
    from_timestamp=from_ts,
    to_timestamp=to_ts,
    num_workers=num_workers,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Review Results

# COMMAND ----------

# Display results summary
print("\n")
print("=" * 60)
print("RESULTS SUMMARY")
print("=" * 60)
print(f"Queries Extracted:        {report.queries_extracted}")
print(f"Complex Queries Found:    {report.complex_queries}")
print(f"Trusted Assets Created:   {report.trusted_assets_created}")
print(f"UC Functions Created:     {report.uc_functions_created}")
print(f"UC Functions Registered:  {report.uc_functions_registered}")
print("=" * 60)

if report.errors:
    print("\n⚠️  ERRORS ENCOUNTERED:")
    for error in report.errors:
        print(f"  - {error}")
else:
    print("\n✅ Completed successfully with no errors!")

if dry_run:
    print("\n📋 This was a DRY RUN - no changes were actually made.")
    print("   Set 'Dry Run' to 'No' to apply changes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC If this was a dry run and you're happy with the results:
# MAGIC 1. Change **"7. Dry Run (preview only)?"** to **"No"**
# MAGIC 2. Re-run the notebook
# MAGIC
# MAGIC ### Verify Your Trusted Assets
# MAGIC
# MAGIC After running (not in dry-run mode):
# MAGIC 1. Open your Genie space
# MAGIC 2. Click **Configure** → **Context** → **SQL Queries**
# MAGIC 3. You should see the newly added SQL examples
# MAGIC
# MAGIC ### Verify Your Functions
# MAGIC
# MAGIC Check Unity Catalog for your new functions:

# COMMAND ----------

# Show created functions (if any were created)
if not dry_run and report.uc_functions_created > 0:
    query = f"""
        SELECT routine_name, routine_type, created, last_altered
        FROM {catalog}.information_schema.routines
        WHERE routine_schema = '{schema}'
        AND routine_name LIKE 'genie_%'
        ORDER BY created DESC
    """
    display(spark.sql(query))
else:
    print("No functions to display (either dry run or no functions created)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC ### Common Issues
# MAGIC
# MAGIC | Issue | Solution |
# MAGIC |-------|----------|
# MAGIC | "Permission denied" | Make sure you have CAN MANAGE on the Genie space and CREATE on the schema |
# MAGIC | "No SQL queries found" | The Genie space may have no conversations, or all queries failed |
# MAGIC | "No complex queries found" | Try setting Complexity Threshold to "moderate" |
# MAGIC | Functions not appearing in Genie | Make sure you provided a Warehouse ID and didn't disable function registration |