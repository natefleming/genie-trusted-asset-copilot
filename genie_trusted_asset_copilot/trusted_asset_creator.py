"""
Trusted asset creator for Genie spaces.

This module creates SQL example instructions (trusted assets) and
Unity Catalog functions from complex SQL queries.
"""

import json
import re
import uuid

import sqlparse
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage, SystemMessage
from loguru import logger
from unitycatalog.ai.core.base import FunctionExecutionResult
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

from genie_trusted_asset_copilot.models import (
    CreationResult,
    ExampleQuestionSQL,
    QueryParameter,
    SqlFunction,
    SQLParameter,
    TrustedAssetCandidate,
)

SQL_CORRECTION_PROMPT = """You are an expert SQL developer. A CREATE FUNCTION statement failed with an error.
Analyze the error and provide a corrected SQL statement.

Common issues to fix:
1. Invalid parameter types - use valid Databricks SQL types (STRING, INT, DOUBLE, DATE, TIMESTAMP, etc.)
2. Syntax errors in the SQL body
3. Missing or extra parentheses
4. Invalid DEFAULT values for parameters
5. Reserved keyword conflicts

Provide ONLY the corrected CREATE FUNCTION statement. No explanation needed."""

USAGE_GUIDANCE_PROMPT = """You are a data analyst helping users understand when to use a SQL query.

Given a question and its SQL query, generate concise usage guidance that explains:
1. When this query is relevant (what business scenarios)
2. What the query returns
3. If parameterized, how to customize the parameters

Keep the guidance to 2-4 sentences. Be specific and actionable.
Do NOT repeat the question. Focus on practical guidance."""


class TrustedAssetCreator:
    """Creates trusted assets and Unity Catalog functions."""

    def __init__(
        self,
        space_id: str,
        catalog: str,
        schema: str,
        client: WorkspaceClient | None = None,
        warehouse_id: str | None = None,
    ) -> None:
        """
        Initialize the trusted asset creator.

        Args:
            space_id: The Genie space ID to add trusted assets to.
            catalog: Unity Catalog name for creating functions.
            schema: Schema name within the catalog for functions.
            client: Optional WorkspaceClient instance.
            warehouse_id: SQL warehouse ID for executing CREATE FUNCTION statements.
        """
        self.space_id = space_id
        self.catalog = catalog
        self.schema = schema
        self.client = client or WorkspaceClient()
        self.warehouse_id = warehouse_id

    def _get_current_space_config(self) -> dict:
        """
        Get the current Genie space configuration.

        Returns:
            The parsed serialized_space configuration.
        """
        space = self.client.genie.get_space(
            space_id=self.space_id,
            include_serialized_space=True,
        )

        if not space.serialized_space:
            return {
                "version": 1,
                "config": {},
                "data_sources": {},
                "instructions": {
                    "text_instructions": [],
                    "example_question_sqls": [],
                    "join_specs": [],
                    "sql_snippets": {},
                },
            }

        return json.loads(space.serialized_space)

    def _generate_unique_id(self) -> str:
        """Generate a unique ID for a trusted asset (32-hex UUID without hyphens)."""
        return uuid.uuid4().hex

    def _format_sql(self, sql: str) -> str:
        """
        Format SQL for readability.

        Args:
            sql: The SQL query string (may be unformatted).

        Returns:
            Formatted SQL string with proper indentation and line breaks.
        """
        return sqlparse.format(
            sql,
            reindent=True,
            keyword_case="upper",
            indent_width=2,
        )

    def _sql_to_lines(self, sql: str) -> list[str]:
        """
        Convert SQL string to list of lines for the API format.

        Formats the SQL first, then splits into lines.

        Args:
            sql: The SQL query string.

        Returns:
            List of SQL lines with newlines preserved.
        """
        # Format the SQL for consistency
        formatted_sql = self._format_sql(sql)

        lines = formatted_sql.split("\n")
        # Add newline to all but the last line
        result = []
        for i, line in enumerate(lines):
            if i < len(lines) - 1:
                result.append(line + "\n")
            else:
                result.append(line)
        return result

    def _normalize_question(self, question: str) -> str:
        """
        Normalize a question for comparison to detect duplicates.

        Args:
            question: The question text.

        Returns:
            Normalized lowercase question without extra whitespace.
        """
        return " ".join(question.lower().split())

    def _map_to_genie_type(self, sql_type: str) -> str:
        """
        Map SQL/extracted type to Genie parameter type_hint.

        Args:
            sql_type: The type from parameter extraction.

        Returns:
            Genie parameter type_hint: STRING, DATE, TIMESTAMP, DECIMAL, or INTEGER.
        """
        type_lower = sql_type.lower()

        # Map to uppercase Genie type_hint values
        if type_lower == "string":
            return "STRING"
        if type_lower == "date":
            return "DATE"
        if type_lower in ("date and time", "timestamp", "datetime"):
            return "TIMESTAMP"
        if type_lower in ("decimal", "double", "float", "number", "numeric"):
            return "DECIMAL"
        if type_lower in ("integer", "int", "bigint", "smallint", "tinyint"):
            return "INTEGER"

        # Default to STRING
        return "STRING"

    def _generate_usage_guidance(
        self,
        candidate: TrustedAssetCandidate,
    ) -> str:
        """
        Generate usage guidance for a trusted asset using ChatDatabricks.

        Args:
            candidate: The candidate with question, SQL, and parameters.

        Returns:
            Generated usage guidance text.
        """
        try:
            llm = ChatDatabricks(
                model="databricks-claude-sonnet-4",
                temperature=0.0,
                max_tokens=500,
            )

            # Build parameter info if available
            param_info = ""
            if candidate.parameters:
                param_list = ", ".join(
                    f"{p.name} ({p.sql_type}): {p.description}"
                    for p in candidate.parameters
                )
                param_info = f"\n\nParameters: {param_list}"

            sql_to_show = candidate.parameterized_sql or candidate.sql

            messages = [
                SystemMessage(content=USAGE_GUIDANCE_PROMPT),
                HumanMessage(
                    content=f"Question: {candidate.question}\n\n"
                    f"SQL:\n```sql\n{sql_to_show}\n```{param_info}"
                ),
            ]

            response = llm.invoke(messages)
            guidance = response.content.strip()

            logger.debug(f"Generated usage guidance: {guidance[:100]}...")
            return guidance

        except Exception as e:
            logger.warning(f"Failed to generate usage guidance: {e}")
            # Return a simple fallback
            return f"Use this query to answer: {candidate.question[:100]}"

    def create_trusted_assets(
        self,
        candidates: list[TrustedAssetCandidate],
        dry_run: bool = False,
        force: bool = False,
    ) -> list[CreationResult]:
        """
        Create SQL example instructions (trusted assets) in the Genie space.

        Args:
            candidates: List of candidates to add as trusted assets.
            dry_run: If True, preview changes without applying them.
            force: If True, replace existing assets instead of skipping.

        Returns:
            List of CreationResult objects indicating success/failure.
        """
        if not candidates:
            logger.info("No candidates to create trusted assets for")
            return []

        results: list[CreationResult] = []

        try:
            # Get current space configuration
            config = self._get_current_space_config()

            # Ensure instructions structure exists
            if "instructions" not in config:
                config["instructions"] = {}
            if "example_question_sqls" not in config["instructions"]:
                config["instructions"]["example_question_sqls"] = []

            existing_examples = config["instructions"]["example_question_sqls"]

            # Build a map of normalized questions to their indices for replacement
            existing_question_map: dict[str, int] = {}
            for i, ex in enumerate(existing_examples):
                normalized_q = self._normalize_question("".join(ex.get("question", [])))
                existing_question_map[normalized_q] = i

            if existing_question_map:
                logger.info(
                    f"Found {len(existing_question_map)} existing trusted assets in space"
                )

            # Add new examples
            new_examples: list[dict] = []
            duplicates_in_candidates: set[str] = set()  # Track duplicates within candidates
            indices_to_remove: list[int] = []  # Track indices to remove for force mode

            for candidate in candidates:
                normalized = self._normalize_question(candidate.question)

                # Check if already exists in the Genie space
                if normalized in existing_question_map:
                    if force:
                        logger.info(
                            f"Replacing existing trusted asset: {candidate.question[:50]}..."
                        )
                        indices_to_remove.append(existing_question_map[normalized])
                    else:
                        logger.info(
                            f"Skipping - trusted asset already exists: {candidate.question[:50]}..."
                        )
                        results.append(
                            CreationResult(
                                success=False,
                                asset_type="trusted_asset",
                                name=candidate.question[:50],
                                error="Trusted asset with this question already exists (not overwriting)",
                            )
                        )
                        continue

                # Check if duplicate within this batch of candidates
                if normalized in duplicates_in_candidates:
                    logger.debug(
                        f"Skipping duplicate within batch: {candidate.question[:50]}..."
                    )
                    continue

                duplicates_in_candidates.add(normalized)

                # Use parameterized SQL if available, otherwise use original
                sql_to_use = candidate.parameterized_sql or candidate.sql

                # Generate usage guidance using ChatDatabricks
                logger.info(f"Generating usage guidance for: {candidate.question[:50]}...")
                usage_guidance = self._generate_usage_guidance(candidate)

                # Convert SQLParameter to QueryParameter for Genie API
                query_params: list[QueryParameter] | None = None
                if candidate.parameters:
                    query_params = [
                        QueryParameter(
                            name=p.name,
                            type_hint=self._map_to_genie_type(p.sql_type),
                        )
                        for p in candidate.parameters
                    ]

                example = ExampleQuestionSQL(
                    id=self._generate_unique_id(),
                    question=[candidate.question],
                    sql=self._sql_to_lines(sql_to_use),
                    usage_guidance=[usage_guidance],
                    parameters=query_params,
                )

                new_examples.append(example.model_dump())
                # Track this as processed to avoid duplicates
                existing_question_map[normalized] = -1  # Mark as processed

                logger.info(f"Adding trusted asset: {candidate.question[:50]}...")
                results.append(
                    CreationResult(
                        success=True,
                        asset_type="trusted_asset",
                        name=candidate.question[:50],
                    )
                )

            if not new_examples:
                logger.info("No new trusted assets to add")
                return results

            if dry_run:
                logger.info(f"[DRY RUN] Would add {len(new_examples)} trusted assets")
                if indices_to_remove:
                    logger.info(
                        f"[DRY RUN] Would replace {len(indices_to_remove)} existing trusted assets"
                    )
                return results

            # Remove old entries that are being replaced (in reverse order to preserve indices)
            if indices_to_remove:
                for idx in sorted(indices_to_remove, reverse=True):
                    del config["instructions"]["example_question_sqls"][idx]
                logger.info(f"Removed {len(indices_to_remove)} existing trusted assets for replacement")

            # Update the configuration with new examples
            config["instructions"]["example_question_sqls"].extend(new_examples)

            # Sort by id (required by Genie API)
            config["instructions"]["example_question_sqls"].sort(key=lambda x: x.get("id", ""))

            # Update the space
            serialized = json.dumps(config)
            self.client.genie.update_space(
                space_id=self.space_id,
                serialized_space=serialized,
            )

            logger.success(f"Added {len(new_examples)} trusted assets to Genie space")

        except Exception as e:
            logger.error(f"Failed to create trusted assets: {e}")
            results.append(
                CreationResult(
                    success=False,
                    asset_type="trusted_asset",
                    name="batch",
                    error=str(e),
                )
            )

        return results

    def _sanitize_function_name(self, question: str) -> str:
        """
        Create a valid function name from a question.

        Args:
            question: The question to convert to a function name.

        Returns:
            A valid SQL function name.
        """
        # Take first few words and convert to snake_case
        words = question.lower().split()[:5]
        name = "_".join(words)
        # Remove non-alphanumeric characters except underscores
        name = re.sub(r"[^a-z0-9_]", "", name)
        # Ensure it doesn't start with a number
        if name and name[0].isdigit():
            name = "fn_" + name
        # Limit length
        name = name[:50]
        # Add prefix for clarity
        return f"genie_{name}"

    def _generate_function_description(
        self,
        candidate: TrustedAssetCandidate,
    ) -> str:
        """
        Generate a clear, concise description for a UC function using LLM.

        Args:
            candidate: The candidate with question and SQL.

        Returns:
            A clear description of what the function does.
        """
        try:
            llm = ChatDatabricks(
                endpoint="databricks-claude-sonnet-4",
                temperature=0.3,
                max_tokens=150,
            )

            messages = [
                SystemMessage(
                    content="Generate a clear, concise 1-2 sentence description of what this SQL function does. "
                    "Focus on the business value and what data it returns. "
                    "Do NOT include the example question. "
                    "Do NOT use markdown or special formatting. "
                    "Write in plain text suitable for a function comment."
                ),
                HumanMessage(
                    content=f"Question: {candidate.question}\n\nSQL:\n{candidate.sql[:500]}"
                ),
            ]

            response = llm.invoke(messages)
            description = response.content.strip()

            # Clean up any markdown or quotes
            description = description.strip('"\'')

            return description

        except Exception as e:
            logger.warning(f"Failed to generate function description: {e}")
            # Fallback to a generic description based on complexity
            return "Executes a complex analytical query based on user requirements."

    def _build_function_comment(
        self,
        candidate: TrustedAssetCandidate,
    ) -> str:
        """
        Build a markdown-formatted description for the UC function.

        Args:
            candidate: The candidate with question and SQL.

        Returns:
            A markdown-formatted description string for the SQL function.
        """
        # Generate an overall description
        description = self._generate_function_description(candidate)

        # Prepare the example question
        question = candidate.question
        if len(question) > 300:
            question = question[:297] + "..."

        # Build markdown-formatted comment
        # Using \\n for newlines in SQL string
        parts = [
            "## Description",
            "",
            description,
            "",
            "## Example Question",
            "",
            f"> {question}",
            "",
            "---",
            "*Auto-generated by genie-trusted-asset-copilot*",
        ]

        full_comment = "\\n".join(parts)

        # Escape single quotes for SQL safety
        return full_comment.replace("'", "''")

    def _convert_sql_placeholder_to_param(self, parameterized_sql: str) -> str:
        """
        Convert :param_name placeholders to SQL parameter references for UC functions.

        Args:
            parameterized_sql: SQL with :param_name placeholders.

        Returns:
            SQL with proper parameter references for UC functions.
        """
        # Unity Catalog SQL functions use the parameter name directly
        # Replace :param_name with param_name
        return re.sub(r':(\w+)', r'\1', parameterized_sql)

    def _build_param_definition(self, param: SQLParameter) -> str:
        """
        Build a parameter definition with inline comment.

        Args:
            param: The SQL parameter.

        Returns:
            Parameter definition string with COMMENT clause.
        """
        # Escape single quotes in description
        desc_escaped = param.description.replace("'", "''")
        default_val = self._format_default_value(param)

        return (
            f"{param.name} {param.sql_type} "
            f"DEFAULT {default_val} "
            f"COMMENT '{desc_escaped}'"
        )

    def _generate_function_sql(
        self,
        candidate: TrustedAssetCandidate,
    ) -> tuple[str, str]:
        """
        Generate a CREATE FUNCTION statement for a complex query.

        Args:
            candidate: The candidate query to convert to a function.

        Returns:
            Tuple of (function_name, CREATE FUNCTION SQL statement).
        """
        func_name = self._sanitize_function_name(candidate.question)
        full_name = f"{self.catalog}.{self.schema}.{func_name}"
        parameters = candidate.parameters

        # Build concise description (the original question)
        comment = self._build_function_comment(candidate)

        # Build parameter list for function signature with inline comments
        if parameters:
            param_defs = ",\n    ".join(
                self._build_param_definition(p) for p in parameters
            )
            param_signature = f"(\n    {param_defs}\n)"

            # Use parameterized SQL if available
            if candidate.parameterized_sql:
                sql_body = self._convert_sql_placeholder_to_param(
                    candidate.parameterized_sql
                )
            else:
                sql_body = candidate.sql
        else:
            param_signature = "()"
            sql_body = candidate.sql

        # Create a table-valued function that returns the query result
        create_sql = f"""CREATE OR REPLACE FUNCTION {full_name}{param_signature}
RETURNS TABLE
LANGUAGE SQL
COMMENT '{comment}'
RETURN ({sql_body})"""

        return func_name, create_sql

    def _format_default_value(self, param: SQLParameter) -> str:
        """
        Format a parameter's default value for SQL.

        Args:
            param: The SQL parameter.

        Returns:
            Properly formatted default value.
        """
        if param.default_value is None:
            return "NULL"

        # String types need quotes
        string_types = {"STRING", "VARCHAR", "CHAR", "TEXT"}
        if param.sql_type.upper() in string_types:
            # Escape single quotes in the value
            escaped = param.default_value.replace("'", "''")
            return f"'{escaped}'"

        # Date/timestamp types need quotes
        date_types = {"DATE", "TIMESTAMP", "DATETIME"}
        if param.sql_type.upper() in date_types:
            return f"'{param.default_value}'"

        # Numeric types don't need quotes
        return param.default_value

    def _attempt_sql_correction(
        self,
        original_sql: str,
        error_message: str,
    ) -> str | None:
        """
        Use LLM to attempt correction of a failed CREATE FUNCTION statement.

        Args:
            original_sql: The original CREATE FUNCTION SQL that failed.
            error_message: The error message from the failed execution.

        Returns:
            Corrected SQL statement, or None if correction failed.
        """
        try:
            llm = ChatDatabricks(
                model="databricks-claude-sonnet-4",
                temperature=0.0,
                max_tokens=2000,
            )

            messages = [
                SystemMessage(content=SQL_CORRECTION_PROMPT),
                HumanMessage(
                    content=f"Original SQL that failed:\n```sql\n{original_sql}\n```\n\n"
                    f"Error message:\n{error_message}\n\n"
                    f"Provide the corrected CREATE FUNCTION statement:"
                ),
            ]

            response = llm.invoke(messages)
            corrected_sql = response.content.strip()

            # Extract SQL from code block if present
            if "```sql" in corrected_sql:
                start = corrected_sql.find("```sql") + 6
                end = corrected_sql.find("```", start)
                if end > start:
                    corrected_sql = corrected_sql[start:end].strip()
            elif "```" in corrected_sql:
                start = corrected_sql.find("```") + 3
                end = corrected_sql.find("```", start)
                if end > start:
                    corrected_sql = corrected_sql[start:end].strip()

            logger.info("LLM suggested a corrected SQL statement")
            return corrected_sql

        except Exception as e:
            logger.warning(f"SQL correction attempt failed: {e}")
            return None

    def _create_function_with_retry(
        self,
        candidate: TrustedAssetCandidate,
        max_retries: int = 2,
    ) -> CreationResult:
        """
        Attempt to create a UC function with retry and error correction.

        Args:
            candidate: The candidate to create a function for.
            max_retries: Maximum number of retry attempts.

        Returns:
            CreationResult indicating success or failure.
        """
        func_name, create_sql = self._generate_function_sql(candidate)
        full_function_name = f"{self.catalog}.{self.schema}.{func_name}"
        current_sql = create_sql
        last_error: str | None = None

        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt}/{max_retries} for {func_name}")

                self.client.statement_execution.execute_statement(
                    warehouse_id=self.warehouse_id,
                    statement=current_sql,
                    catalog=self.catalog,
                    schema=self.schema,
                    wait_timeout="50s",
                )

                logger.success(f"Created UC function: {func_name}")

                # Set tags to indicate auto-generation
                self._set_function_tags(full_function_name)

                # Test the function
                test_passed, test_error = self._test_function(full_function_name)

                if test_passed:
                    return CreationResult(
                        success=True,
                        asset_type="uc_function",
                        name=func_name,
                    )
                else:
                    return CreationResult(
                        success=True,
                        asset_type="uc_function",
                        name=func_name,
                        error=f"Created but test failed: {test_error}",
                    )

            except Exception as e:
                last_error = str(e)
                logger.warning(f"Function creation failed (attempt {attempt + 1}): {e}")

                # Try to correct the SQL if we have retries left
                if attempt < max_retries:
                    corrected_sql = self._attempt_sql_correction(current_sql, last_error)
                    if corrected_sql and corrected_sql != current_sql:
                        current_sql = corrected_sql
                        logger.info("Applying corrected SQL for next attempt")
                    elif candidate.parameters and attempt == 0:
                        # Fall back to non-parameterized version
                        logger.info(
                            "Falling back to non-parameterized function"
                        )
                        # Generate function without parameters
                        func_name_simple = self._sanitize_function_name(candidate.question)
                        full_name_simple = f"{self.catalog}.{self.schema}.{func_name_simple}"
                        comment = self._build_function_comment(candidate)
                        current_sql = f"""CREATE OR REPLACE FUNCTION {full_name_simple}()
RETURNS TABLE
LANGUAGE SQL
COMMENT '{comment}'
RETURN ({candidate.sql})"""
                    else:
                        # No correction available, break
                        break

        # All retries exhausted
        return CreationResult(
            success=False,
            asset_type="uc_function",
            name=func_name,
            error=f"Failed after {max_retries + 1} attempts: {last_error}",
        )

    def _set_function_tags(self, function_name: str) -> None:
        """
        Set tags on a UC function to indicate it was auto-generated.

        Args:
            function_name: The full function name (catalog.schema.function).
        """
        try:
            tag_sql = f"""
                ALTER FUNCTION {function_name}
                SET TAGS (
                    'generated_by' = 'genie-trusted-asset-copilot',
                    'auto_generated' = 'true',
                    'source' = 'genie_conversation'
                )
            """

            self.client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=tag_sql,
                catalog=self.catalog,
                schema=self.schema,
                wait_timeout="30s",
            )

            logger.debug(f"Set tags on function: {function_name}")

        except Exception as e:
            # Tags are non-critical, just log a warning
            logger.warning(f"Failed to set tags on function {function_name}: {e}")

    def _test_function(self, function_name: str) -> tuple[bool, str | None]:
        """
        Test a UC function by executing it.

        Args:
            function_name: The full function name (catalog.schema.function).

        Returns:
            Tuple of (success: bool, error_message: str | None).
        """
        try:
            dfs = DatabricksFunctionClient(client=self.client)

            logger.info(f"Testing function: {function_name}")
            result: FunctionExecutionResult = dfs.execute_function(
                function_name=function_name,
                parameters={},
            )

            if result.error:
                logger.error(f"Function test failed: {result.error}")
                return False, str(result.error)

            # Log success with result preview
            result_str = str(result.value) if result.value else "No results"
            if len(result_str) > 200:
                result_str = result_str[:200] + "..."

            logger.success(f"Function test passed: {function_name}")
            logger.debug(f"Test result preview: {result_str}")
            return True, None

        except Exception as e:
            error_msg = f"Function test execution error: {e}"
            logger.error(error_msg)
            return False, error_msg

    def create_uc_functions(
        self,
        candidates: list[TrustedAssetCandidate],
        dry_run: bool = False,
        force: bool = False,  # noqa: ARG002 - UC functions use CREATE OR REPLACE
    ) -> list[CreationResult]:
        """
        Create Unity Catalog functions from complex queries.

        Note: UC functions always use CREATE OR REPLACE, so existing functions
        are automatically replaced. The force parameter is accepted for API
        consistency but has no effect.

        Args:
            candidates: List of candidates to create functions for.
            dry_run: If True, preview changes without applying them.
            force: Accepted for API consistency (UC functions always replace).

        Returns:
            List of CreationResult objects indicating success/failure.
        """
        if not candidates:
            logger.info("No candidates to create UC functions for")
            return []

        if not self.warehouse_id:
            logger.warning("No warehouse_id provided, skipping UC function creation")
            return [
                CreationResult(
                    success=False,
                    asset_type="uc_function",
                    name="all",
                    error="warehouse_id not provided",
                )
            ]

        results: list[CreationResult] = []
        created_names: set[str] = set()

        for candidate in candidates:
            func_name = self._sanitize_function_name(candidate.question)

            # Skip if we already created a function with this name
            if func_name in created_names:
                logger.debug(f"Skipping duplicate function name: {func_name}")
                continue

            created_names.add(func_name)

            if dry_run:
                _, create_sql = self._generate_function_sql(candidate)
                params_info = (
                    f" with {len(candidate.parameters)} parameters"
                    if candidate.parameters
                    else ""
                )
                logger.info(f"[DRY RUN] Would create UC function: {func_name}{params_info}")
                logger.debug(f"SQL:\n{create_sql}")
                results.append(
                    CreationResult(
                        success=True,
                        asset_type="uc_function",
                        name=func_name,
                    )
                )
                continue

            # Use retry mechanism with error correction
            result = self._create_function_with_retry(candidate, max_retries=2)
            results.append(result)

        successful = sum(1 for r in results if r.success)
        logger.info(f"Created {successful}/{len(results)} UC functions")

        return results

    def register_functions_with_genie(
        self,
        function_names: list[str],
        dry_run: bool = False,
        force: bool = False,
    ) -> list[CreationResult]:
        """
        Register UC functions with the Genie room.

        Args:
            function_names: List of full function names (catalog.schema.function_name).
            dry_run: If True, preview changes without applying them.
            force: If True, replace existing registrations.

        Returns:
            List of CreationResult objects indicating success/failure.
        """
        if not function_names:
            logger.info("No functions to register with Genie room")
            return []

        results: list[CreationResult] = []

        try:
            # Get current space configuration
            config = self._get_current_space_config()

            # Ensure instructions structure exists
            if "instructions" not in config:
                config["instructions"] = {}
            if "sql_functions" not in config["instructions"]:
                config["instructions"]["sql_functions"] = []

            existing_functions = config["instructions"]["sql_functions"]

            # Build map of existing function identifiers
            existing_identifiers = {
                func.get("identifier", ""): i for i, func in enumerate(existing_functions)
            }

            if existing_identifiers:
                logger.info(
                    f"Found {len(existing_identifiers)} existing registered functions in space"
                )

            new_functions: list[dict] = []
            indices_to_remove: list[int] = []

            for func_name in function_names:
                # Check if already registered
                if func_name in existing_identifiers:
                    if force:
                        logger.info(f"Replacing existing function registration: {func_name}")
                        indices_to_remove.append(existing_identifiers[func_name])
                    else:
                        logger.info(
                            f"Skipping - function already registered: {func_name}"
                        )
                        results.append(
                            CreationResult(
                                success=False,
                                asset_type="function_registration",
                                name=func_name,
                                error="Function already registered (use --force to replace)",
                            )
                        )
                        continue

                # Create new registration entry
                sql_func = SqlFunction(
                    id=self._generate_unique_id(),
                    identifier=func_name,
                )

                new_functions.append(sql_func.model_dump())

                logger.info(f"Registering function with Genie: {func_name}")
                results.append(
                    CreationResult(
                        success=True,
                        asset_type="function_registration",
                        name=func_name,
                    )
                )

            if not new_functions:
                logger.info("No new functions to register")
                return results

            if dry_run:
                logger.info(f"[DRY RUN] Would register {len(new_functions)} functions")
                if indices_to_remove:
                    logger.info(
                        f"[DRY RUN] Would replace {len(indices_to_remove)} existing registrations"
                    )
                return results

            # Remove old entries being replaced (in reverse order)
            if indices_to_remove:
                for idx in sorted(indices_to_remove, reverse=True):
                    del config["instructions"]["sql_functions"][idx]
                logger.info(
                    f"Removed {len(indices_to_remove)} existing registrations for replacement"
                )

            # Add new function registrations
            config["instructions"]["sql_functions"].extend(new_functions)

            # Sort by id (required by Genie API)
            config["instructions"]["sql_functions"].sort(key=lambda x: x.get("id", ""))

            # Update the space
            serialized = json.dumps(config)
            self.client.genie.update_space(
                space_id=self.space_id,
                serialized_space=serialized,
            )

            logger.success(f"Registered {len(new_functions)} functions with Genie room")

        except Exception as e:
            logger.error(f"Failed to register functions: {e}")
            results.append(
                CreationResult(
                    success=False,
                    asset_type="function_registration",
                    name="batch",
                    error=str(e),
                )
            )

        return results

    def create_all(
        self,
        candidates: list[TrustedAssetCandidate],
        dry_run: bool = False,
        force: bool = False,
        create_sql_instructions: bool = True,
        create_uc_functions: bool = True,
        register_uc_functions: bool = True,
    ) -> tuple[list[CreationResult], list[CreationResult], list[CreationResult]]:
        """
        Create trusted assets, UC functions, and register functions with Genie.

        Args:
            candidates: List of candidates to process.
            dry_run: If True, preview changes without applying them.
            force: If True, replace existing assets instead of skipping.
            create_sql_instructions: Create SQL example instructions in Genie room.
            create_uc_functions: Create UC functions in Unity Catalog.
            register_uc_functions: Register UC functions with Genie room.

        Returns:
            Tuple of (trusted_asset_results, uc_function_results, register_results).
        """
        logger.info(f"Creating assets for {len(candidates)} candidates (dry_run={dry_run})")

        # Create SQL instructions (trusted assets)
        if create_sql_instructions:
            trusted_results = self.create_trusted_assets(
                candidates, dry_run=dry_run, force=force
            )
        else:
            logger.info("Skipping SQL instruction creation (--no-sql-instructions)")
            trusted_results = []

        # Create UC functions
        if create_uc_functions:
            uc_results = self.create_uc_functions(candidates, dry_run=dry_run, force=force)
        else:
            logger.info("Skipping UC function creation (--no-uc-functions)")
            uc_results = []

        # Register functions with Genie
        register_results: list[CreationResult] = []
        if register_uc_functions and uc_results:
            # Get the names of successfully created functions
            created_functions = [
                f"{self.catalog}.{self.schema}.{r.name}"
                for r in uc_results
                if r.success
            ]
            if created_functions:
                register_results = self.register_functions_with_genie(
                    created_functions, dry_run=dry_run, force=force
                )
        elif not register_uc_functions:
            logger.info("Skipping function registration (--no-register-functions)")

        return trusted_results, uc_results, register_results
