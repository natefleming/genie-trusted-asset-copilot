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
# MAGIC 1. Fill in the parameters in the widgets at the top of the notebook
# MAGIC 2. Run all cells
# MAGIC 3. Review the results at the bottom

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure Parameters
# MAGIC 
# MAGIC Use the widgets above to set your parameters, or modify the defaults below.

# COMMAND ----------

# Create widgets for all parameters
dbutils.widgets.text("space_id", "", "1. Genie Space ID")
dbutils.widgets.text("catalog", "", "2. Unity Catalog Name")
dbutils.widgets.text("schema", "", "3. Schema Name")
dbutils.widgets.text("warehouse_id", "", "4. SQL Warehouse ID (optional)")
dbutils.widgets.text("max_conversations", "", "5. Max Conversations (optional)")
dbutils.widgets.dropdown("complexity_threshold", "complex", ["simple", "moderate", "complex"], "6. Complexity Threshold")
dbutils.widgets.dropdown("dry_run", "Yes", ["Yes", "No"], "7. Dry Run (preview only)?")
dbutils.widgets.dropdown("force_replace", "No", ["Yes", "No"], "8. Force Replace Existing?")
dbutils.widgets.dropdown("create_sql_instructions", "Yes", ["Yes", "No"], "9. Create SQL Instructions?")
dbutils.widgets.dropdown("create_uc_functions", "Yes", ["Yes", "No"], "10. Create UC Functions?")
dbutils.widgets.dropdown("register_functions", "Yes", ["Yes", "No"], "11. Register Functions with Genie?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Install Dependencies
# MAGIC 
# MAGIC This cell installs the required packages. It only needs to run once per cluster session.

# COMMAND ----------

# Install the genie-trusted-asset-copilot package
# Note: Update the path if you've installed the package elsewhere
%pip install /Workspace/Repos/genie-trusted-asset-copilot --quiet

# If running from source, you can also install dependencies directly:
# %pip install databricks-sdk langchain-databricks pydantic loguru sqlparse unitycatalog-ai[databricks] --quiet

# COMMAND ----------

# Restart Python to pick up new packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run the Copilot
# MAGIC 
# MAGIC This cell reads the parameters and runs the trusted asset creation workflow.

# COMMAND ----------

# Re-create widgets after Python restart
dbutils.widgets.text("space_id", "", "1. Genie Space ID")
dbutils.widgets.text("catalog", "", "2. Unity Catalog Name")
dbutils.widgets.text("schema", "", "3. Schema Name")
dbutils.widgets.text("warehouse_id", "", "4. SQL Warehouse ID (optional)")
dbutils.widgets.text("max_conversations", "", "5. Max Conversations (optional)")
dbutils.widgets.dropdown("complexity_threshold", "complex", ["simple", "moderate", "complex"], "6. Complexity Threshold")
dbutils.widgets.dropdown("dry_run", "Yes", ["Yes", "No"], "7. Dry Run (preview only)?")
dbutils.widgets.dropdown("force_replace", "No", ["Yes", "No"], "8. Force Replace Existing?")
dbutils.widgets.dropdown("create_sql_instructions", "Yes", ["Yes", "No"], "9. Create SQL Instructions?")
dbutils.widgets.dropdown("create_uc_functions", "Yes", ["Yes", "No"], "10. Create UC Functions?")
dbutils.widgets.dropdown("register_functions", "Yes", ["Yes", "No"], "11. Register Functions with Genie?")

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

# Convert max_conversations to int if provided
max_conversations = int(max_conversations_str) if max_conversations_str else None

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
print(f"Genie Space ID:        {space_id}")
print(f"Target Location:       {catalog}.{schema}")
print(f"SQL Warehouse ID:      {warehouse_id or '(not provided)'}")
print(f"Max Conversations:     {max_conversations or 'All'}")
print(f"Complexity Threshold:  {complexity_threshold}")
print(f"Dry Run:               {dry_run}")
print(f"Force Replace:         {force_replace}")
print(f"Create SQL Instructions: {create_sql_instructions}")
print(f"Create UC Functions:   {create_uc_functions}")
print(f"Register Functions:    {register_functions}")
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
    display(spark.sql(f"SHOW FUNCTIONS IN {catalog}.{schema} LIKE 'genie_*'"))
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
