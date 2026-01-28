"""
SQL complexity evaluator using ChatDatabricks.

This module analyzes SQL queries to determine their complexity
and extracts parameterizable values using an LLM for structured analysis.
"""

from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage, SystemMessage
from loguru import logger

from genie_trusted_asset_copilot.models import (
    ComplexityAnalysis,
    ExtractedQuery,
    ParameterExtraction,
    SQLComplexity,
    SQLParameter,
    TrustedAssetCandidate,
)

COMPLEXITY_SYSTEM_PROMPT = """You are an expert SQL analyst. Your task is to analyze SQL queries and determine their complexity.

Classify queries into one of three complexity levels:
- SIMPLE: Basic SELECT with simple WHERE clauses, no JOINs or subqueries
- MODERATE: Contains JOINs, GROUP BY, or simple aggregations
- COMPLEX: Contains multiple JOINs, subqueries, CTEs, window functions, or complex aggregations

When analyzing, identify:
1. Number of JOIN operations
2. Presence of subqueries (in SELECT, FROM, or WHERE clauses)
3. Use of CTEs (WITH clauses)
4. Window functions (OVER, PARTITION BY, ROW_NUMBER, RANK, etc.)
5. Complex aggregations (GROUP BY with HAVING, multiple aggregate functions)
6. Set operations (UNION, INTERSECT, EXCEPT)

A query should be classified as COMPLEX if it has:
- 3 or more JOINs, OR
- Any CTEs with multiple references, OR
- Window functions, OR
- Nested subqueries, OR
- Complex business logic that would benefit from being a reusable trusted asset

Provide clear reasoning for your classification."""

PARAMETER_EXTRACTION_PROMPT = """You are an expert SQL analyst. Your task is to identify literal values in SQL queries that should be parameterized for reusability.

Identify values that are likely to change between executions:
1. **Dates and time periods**: Specific dates, date ranges, week numbers, months, years
2. **Entity identifiers**: Customer IDs, airline names, site names, region names, product codes
3. **Thresholds and limits**: Numeric thresholds, LIMIT values, TOP N values
4. **Status values**: Status codes, categories that might be filtered differently

DO NOT parameterize:
- Table names or column names
- SQL keywords or operators
- Aggregate functions
- Constant business logic values that are unlikely to change

For each parameter:
1. Create a descriptive snake_case name (e.g., start_date, airline_name, min_threshold)
2. Determine the Genie parameter type (MUST be one of): String, Date, Date and Time, Decimal, or Integer
3. Use the original value as the default value
4. Write a clear description of what the parameter represents

In the parameterized SQL:
- Replace literal values with named parameter markers using colon prefix: :parameter_name
- Maintain proper SQL syntax
- Keep the query structure intact

Example:
Original: SELECT * FROM orders WHERE order_date >= '2024-01-01' AND customer = 'ACME Corp' AND amount > 1000
Parameterized: SELECT * FROM orders WHERE order_date >= :start_date AND customer = :customer_name AND amount > :min_amount

Parameters:
- start_date (Date): The minimum order date filter, default '2024-01-01'
- customer_name (String): Customer name to filter by, default 'ACME Corp'
- min_amount (Integer): Minimum order amount threshold, default 1000"""


class ComplexityEvaluator:
    """Evaluates SQL query complexity using ChatDatabricks."""

    def __init__(
        self,
        model: str = "databricks-claude-sonnet-4",
        temperature: float = 0.0,
        max_tokens: int = 1000,
    ) -> None:
        """
        Initialize the complexity evaluator.

        Args:
            model: The Databricks model to use for analysis.
            temperature: LLM temperature (0 for deterministic output).
            max_tokens: Maximum tokens in the response.
        """
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens

        self._llm: ChatDatabricks | None = None
        self._structured_llm: ChatDatabricks | None = None
        self._param_extraction_llm: ChatDatabricks | None = None

    @property
    def llm(self) -> ChatDatabricks:
        """Lazy initialization of the LLM client."""
        if self._llm is None:
            self._llm = ChatDatabricks(
                model=self.model,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )
        return self._llm

    @property
    def structured_llm(self) -> ChatDatabricks:
        """LLM configured for structured output for complexity analysis."""
        if self._structured_llm is None:
            self._structured_llm = self.llm.with_structured_output(ComplexityAnalysis)
        return self._structured_llm

    @property
    def param_extraction_llm(self) -> ChatDatabricks:
        """LLM configured for structured output for parameter extraction."""
        if self._param_extraction_llm is None:
            self._param_extraction_llm = self.llm.with_structured_output(
                ParameterExtraction
            )
        return self._param_extraction_llm

    def extract_parameters(
        self,
        sql: str,
        question: str,
    ) -> tuple[list[SQLParameter], str | None]:
        """
        Extract parameterizable values from a SQL query.

        Args:
            sql: The SQL query to analyze.
            question: The original question for context.

        Returns:
            Tuple of (list of extracted parameters, parameterized SQL or None).
        """
        messages = [
            SystemMessage(content=PARAMETER_EXTRACTION_PROMPT),
            HumanMessage(
                content=f"Extract parameters from this SQL query.\n\n"
                f"Original question: {question}\n\n"
                f"SQL:\n```sql\n{sql}\n```"
            ),
        ]

        try:
            result = self.param_extraction_llm.invoke(messages)

            if isinstance(result, ParameterExtraction):
                if result.parameters:
                    logger.info(
                        f"Extracted {len(result.parameters)} parameters: "
                        f"{', '.join(p.name for p in result.parameters)}"
                    )
                    return result.parameters, result.parameterized_sql
                else:
                    logger.debug("No parameterizable values found in query")
                    return [], None

            if isinstance(result, dict):
                extraction = ParameterExtraction(**result)
                return extraction.parameters, extraction.parameterized_sql

            logger.warning("Unexpected result type from parameter extraction LLM")
            return [], None

        except Exception as e:
            logger.warning(f"Parameter extraction failed: {e}")
            return [], None

    def analyze_query(self, sql: str) -> ComplexityAnalysis:
        """
        Analyze a SQL query and return its complexity classification.

        Args:
            sql: The SQL query to analyze.

        Returns:
            ComplexityAnalysis with the complexity classification and details.
        """
        messages = [
            SystemMessage(content=COMPLEXITY_SYSTEM_PROMPT),
            HumanMessage(content=f"Analyze this SQL query:\n\n```sql\n{sql}\n```"),
        ]

        try:
            result = self.structured_llm.invoke(messages)

            # The structured output should return a ComplexityAnalysis
            if isinstance(result, ComplexityAnalysis):
                return result

            # Handle case where result is a dict (shouldn't happen with structured output)
            if isinstance(result, dict):
                return ComplexityAnalysis(**result)

            # Fallback to simple classification
            logger.warning("Unexpected result type from LLM, falling back to simple analysis")
            return self._fallback_analysis(sql)

        except Exception as e:
            logger.warning(f"LLM analysis failed, using fallback: {e}")
            return self._fallback_analysis(sql)

    def _fallback_analysis(self, sql: str) -> ComplexityAnalysis:
        """
        Perform simple regex-based complexity analysis as fallback.

        Args:
            sql: The SQL query to analyze.

        Returns:
            ComplexityAnalysis based on keyword detection.
        """
        sql_upper = sql.upper()

        # Count JOINs
        join_count = sql_upper.count(" JOIN ")
        has_joins = join_count > 0

        # Check for subqueries
        has_subqueries = "SELECT" in sql_upper[sql_upper.find("FROM") :] if "FROM" in sql_upper else False

        # Check for CTEs
        has_ctes = sql_upper.strip().startswith("WITH ")

        # Check for window functions
        window_keywords = ["OVER(", "OVER (", "PARTITION BY", "ROW_NUMBER", "RANK(", "DENSE_RANK", "LAG(", "LEAD("]
        has_window_functions = any(kw in sql_upper for kw in window_keywords)

        # Check for aggregations
        agg_keywords = ["GROUP BY", "SUM(", "COUNT(", "AVG(", "MAX(", "MIN("]
        has_aggregations = any(kw in sql_upper for kw in agg_keywords)

        # Determine complexity
        if has_window_functions or has_ctes or join_count >= 3 or has_subqueries:
            complexity = SQLComplexity.COMPLEX
            reasoning = "Query contains advanced SQL features (window functions, CTEs, multiple JOINs, or subqueries)"
        elif has_joins or has_aggregations:
            complexity = SQLComplexity.MODERATE
            reasoning = "Query contains JOINs or aggregations"
        else:
            complexity = SQLComplexity.SIMPLE
            reasoning = "Simple query with basic SELECT/WHERE operations"

        return ComplexityAnalysis(
            complexity=complexity,
            reasoning=reasoning,
            has_joins=has_joins,
            has_subqueries=has_subqueries,
            has_ctes=has_ctes,
            has_window_functions=has_window_functions,
            has_aggregations=has_aggregations,
            join_count=join_count,
        )

    def _log_analysis_result(
        self,
        query: ExtractedQuery,
        analysis: ComplexityAnalysis,
    ) -> None:
        """
        Log the SQL query with its complexity analysis.

        Args:
            query: The extracted query that was analyzed.
            analysis: The complexity analysis result.
        """
        # Truncate SQL for display (show first 500 chars)
        sql_display = query.sql[:500]
        if len(query.sql) > 500:
            sql_display += "\n    ... (truncated)"

        # Build feature summary
        features = []
        if analysis.has_joins:
            features.append(f"JOINs: {analysis.join_count}")
        if analysis.has_ctes:
            features.append("CTEs")
        if analysis.has_window_functions:
            features.append("Window Functions")
        if analysis.has_subqueries:
            features.append("Subqueries")
        if analysis.has_aggregations:
            features.append("Aggregations")

        features_str = ", ".join(features) if features else "None detected"

        # Log based on complexity level
        log_msg = (
            f"\n{'=' * 70}\n"
            f"COMPLEXITY: {analysis.complexity.value.upper()}\n"
            f"{'=' * 70}\n"
            f"Question: {query.question[:100]}{'...' if len(query.question) > 100 else ''}\n"
            f"Features: {features_str}\n"
            f"Reasoning: {analysis.reasoning}\n"
            f"SQL:\n    {sql_display.replace(chr(10), chr(10) + '    ')}\n"
            f"{'=' * 70}"
        )

        if analysis.complexity == SQLComplexity.COMPLEX:
            logger.info(log_msg)
        else:
            logger.debug(log_msg)

    def evaluate_queries(
        self,
        queries: list[ExtractedQuery],
        complexity_threshold: SQLComplexity = SQLComplexity.COMPLEX,
    ) -> list[TrustedAssetCandidate]:
        """
        Evaluate multiple queries and return candidates meeting the complexity threshold.

        Args:
            queries: List of extracted queries to evaluate.
            complexity_threshold: Minimum complexity to be considered a candidate.

        Returns:
            List of TrustedAssetCandidate objects for complex queries.
        """
        candidates: list[TrustedAssetCandidate] = []
        complexity_order = {
            SQLComplexity.SIMPLE: 0,
            SQLComplexity.MODERATE: 1,
            SQLComplexity.COMPLEX: 2,
        }
        threshold_value = complexity_order[complexity_threshold]

        logger.info(f"Evaluating {len(queries)} queries for complexity")

        for i, query in enumerate(queries):
            logger.info(f"Analyzing query {i + 1}/{len(queries)}: {query.question[:60]}...")

            analysis = self.analyze_query(query.sql)

            # Log the SQL, complexity, and reasoning for every query
            self._log_analysis_result(query, analysis)

            if complexity_order[analysis.complexity] >= threshold_value:
                # Extract parameters for complex queries
                logger.info("Extracting parameters for complex query...")
                parameters, parameterized_sql = self.extract_parameters(
                    query.sql, query.question
                )

                candidates.append(
                    TrustedAssetCandidate(
                        question=query.question,
                        sql=query.sql,
                        complexity=analysis,
                        execution_time_ms=query.execution_time_ms,
                        message_id=query.message_id,
                        conversation_id=query.conversation_id,
                        parameters=parameters,
                        parameterized_sql=parameterized_sql,
                    )
                )

        logger.info(
            f"Found {len(candidates)} complex queries out of {len(queries)} total"
        )
        return candidates
