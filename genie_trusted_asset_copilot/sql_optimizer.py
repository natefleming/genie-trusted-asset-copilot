"""
SQL optimization using sqlglot.

Optimizes SQL queries by:
- Simplifying expressions
- Removing redundant operations
- Validating syntax
- Normalizing formatting
"""

import sqlglot
from loguru import logger


class SQLOptimizer:
    """Optimizes SQL queries using sqlglot for Databricks DBSQL dialect."""

    def __init__(self, dialect: str = "databricks") -> None:
        """
        Initialize optimizer with target SQL dialect.

        Args:
            dialect: SQL dialect to use. Default "databricks" for Databricks DBSQL.
        """
        self.dialect = dialect

    def optimize(self, sql: str) -> tuple[str, bool, list[str]]:
        """
        Optimize a SQL query.

        Args:
            sql: The SQL query to optimize.

        Returns:
            Tuple of (optimized_sql, was_optimized, optimizations_applied)
        """
        try:
            # Parse SQL into AST
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            # Apply optimizations
            optimized = sqlglot.optimize(
                parsed,
                dialect=self.dialect,
                rules=[
                    "simplify",  # Simplify expressions
                    "unnest_subqueries",  # Remove unnecessary subqueries
                    "normalize",  # Normalize structure
                    "qualify",  # Fully qualify columns
                    "pushdown_projections",  # Push down SELECT
                    "eliminate_joins",  # Remove redundant joins
                    "eliminate_ctes",  # Remove unused CTEs
                    "merge_subqueries",  # Merge redundant subqueries
                ],
            )

            # Format optimized SQL
            optimized_sql = optimized.sql(dialect=self.dialect, pretty=True)

            # Check if optimization changed the query
            was_optimized = optimized_sql != sql

            # Track what changed
            optimizations = []
            if was_optimized:
                # Compare structures
                if "WITH" in sql and "WITH" not in optimized_sql:
                    optimizations.append("Eliminated CTE")
                if sql.count("SELECT") > optimized_sql.count("SELECT"):
                    optimizations.append("Removed subquery")
                if len(optimized_sql) < len(sql) * 0.9:
                    optimizations.append("Simplified query structure")

            return optimized_sql, was_optimized, optimizations

        except Exception as e:
            logger.warning(f"SQL optimization failed: {e}. Using original SQL.")
            return sql, False, []

    def validate(self, sql: str) -> tuple[bool, str | None]:
        """
        Validate SQL syntax.

        Args:
            sql: The SQL query to validate.

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            sqlglot.parse_one(sql, dialect=self.dialect)
            return True, None
        except Exception as e:
            return False, str(e)
