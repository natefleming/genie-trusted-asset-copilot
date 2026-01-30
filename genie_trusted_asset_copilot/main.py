"""
CLI entry point for Genie Trusted Asset Copilot.

This module orchestrates the workflow of extracting SQL queries from
Genie conversations, analyzing their complexity, and creating trusted
assets and Unity Catalog functions.
"""

import argparse
import sys
from datetime import datetime, timezone

from loguru import logger

from genie_trusted_asset_copilot.complexity_evaluator import ComplexityEvaluator
from genie_trusted_asset_copilot.conversation_reader import (
    ConversationReader,
    parse_timestamp,
)
from genie_trusted_asset_copilot.logging_config import configure_logging
from genie_trusted_asset_copilot.models import ProcessingReport, SQLComplexity
from genie_trusted_asset_copilot.trusted_asset_creator import TrustedAssetCreator


def run(
    space_id: str,
    catalog: str,
    schema: str,
    warehouse_id: str | None = None,
    max_conversations: int | None = None,
    include_all_users: bool = False,
    model: str = "databricks-claude-sonnet-4",
    complexity_threshold: str = "complex",
    dry_run: bool = False,
    force: bool = False,
    create_sql_instructions: bool = True,
    create_uc_functions: bool = True,
    register_uc_functions: bool = True,
    from_timestamp: int | None = None,
    to_timestamp: int | None = None,
    optimize_sql: bool = False,
) -> ProcessingReport:
    """
    Run the trusted asset creation workflow.

    Args:
        space_id: The Genie space ID to process.
        catalog: Unity Catalog name for creating functions.
        schema: Schema name within the catalog.
        warehouse_id: SQL warehouse ID for executing statements.
        max_conversations: Maximum number of conversations to process.
        include_all_users: Include conversations from all users.
        model: Databricks model for complexity analysis.
        complexity_threshold: Minimum complexity level (simple, moderate, complex).
        dry_run: Preview changes without applying them.
        force: Replace existing assets instead of skipping.
        create_sql_instructions: Create SQL example instructions in Genie room.
        create_uc_functions: Create UC functions in Unity Catalog.
        register_uc_functions: Register UC functions with Genie room.
        from_timestamp: Optional start timestamp in milliseconds (inclusive).
        to_timestamp: Optional end timestamp in milliseconds (inclusive).
        optimize_sql: Whether to optimize SQL queries before creating assets (default: False).

    Returns:
        ProcessingReport with summary statistics.
    """
    logger.info("Starting Genie Trusted Asset Copilot")
    logger.info(f"Space ID: {space_id}")
    logger.info(f"Target catalog.schema: {catalog}.{schema}")
    logger.info(f"Complexity threshold: {complexity_threshold}")
    logger.info(f"Dry run: {dry_run}")

    errors: list[str] = []

    # Step 1: Read conversations and extract SQL queries
    logger.info("Step 1: Reading conversations and extracting SQL queries...")
    reader = ConversationReader(
        space_id=space_id,
        include_all_users=include_all_users,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
    )

    try:
        queries = reader.extract_all_queries(max_conversations=max_conversations)
    except Exception as e:
        error_msg = f"Failed to extract queries: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
        return ProcessingReport(
            total_conversations=0,
            total_messages=0,
            queries_extracted=0,
            complex_queries=0,
            trusted_assets_created=0,
            uc_functions_created=0,
            errors=errors,
        )

    if not queries:
        logger.warning("No SQL queries found in conversations")
        return ProcessingReport(
            total_conversations=max_conversations or 0,
            total_messages=0,
            queries_extracted=0,
            complex_queries=0,
            trusted_assets_created=0,
            uc_functions_created=0,
            errors=errors,
        )

    logger.info(f"Extracted {len(queries)} SQL queries")

    # Step 2: Analyze complexity
    logger.info("Step 2: Analyzing SQL complexity...")
    evaluator = ComplexityEvaluator(model=model, optimize_sql=optimize_sql)

    threshold = SQLComplexity(complexity_threshold.lower())
    candidates = evaluator.evaluate_queries(queries, complexity_threshold=threshold)

    if not candidates:
        logger.warning(f"No queries met the {complexity_threshold} complexity threshold")
        return ProcessingReport(
            total_conversations=max_conversations or 0,
            total_messages=len(queries),
            queries_extracted=len(queries),
            complex_queries=0,
            trusted_assets_created=0,
            uc_functions_created=0,
            errors=errors,
        )

    logger.info(f"Found {len(candidates)} complex queries")

    # Step 3: Create trusted assets and UC functions
    logger.info("Step 3: Creating trusted assets and UC functions...")
    creator = TrustedAssetCreator(
        space_id=space_id,
        catalog=catalog,
        schema=schema,
        warehouse_id=warehouse_id,
    )

    trusted_results, uc_results, register_results = creator.create_all(
        candidates,
        dry_run=dry_run,
        force=force,
        create_sql_instructions=create_sql_instructions,
        create_uc_functions=create_uc_functions,
        register_uc_functions=register_uc_functions,
    )

    # Count successes
    trusted_created = sum(1 for r in trusted_results if r.success)
    uc_created = sum(1 for r in uc_results if r.success)
    uc_registered = sum(1 for r in register_results if r.success)

    # Collect errors
    for result in trusted_results + uc_results + register_results:
        if not result.success and result.error:
            errors.append(f"{result.asset_type} '{result.name}': {result.error}")

    # Build report
    report = ProcessingReport(
        total_conversations=max_conversations or len(queries),
        total_messages=len(queries),
        queries_extracted=len(queries),
        complex_queries=len(candidates),
        trusted_assets_created=trusted_created,
        uc_functions_created=uc_created,
        uc_functions_registered=uc_registered,
        errors=errors,
    )

    # Log summary
    logger.info("=" * 60)
    logger.info("Processing Complete - Summary")
    logger.info("=" * 60)
    logger.info(f"Queries extracted: {report.queries_extracted}")
    logger.info(f"Complex queries found: {report.complex_queries}")
    logger.info(f"Trusted assets created: {report.trusted_assets_created}")
    logger.info(f"UC functions created: {report.uc_functions_created}")
    logger.info(f"UC functions registered: {report.uc_functions_registered}")
    if errors:
        logger.warning(f"Errors encountered: {len(errors)}")
        for err in errors[:5]:  # Show first 5 errors
            logger.warning(f"  - {err}")
        if len(errors) > 5:
            logger.warning(f"  ... and {len(errors) - 5} more errors")
    logger.info("=" * 60)

    return report


def main() -> int:
    """CLI entrypoint."""
    configure_logging()

    parser = argparse.ArgumentParser(
        description="Automatically create Genie trusted assets and UC functions from complex SQL queries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze a Genie space and create trusted assets (dry run)
  genie-trusted-asset-copilot --space-id abc123 --catalog main --schema genie_functions --dry-run

  # Create trusted assets and UC functions
  genie-trusted-asset-copilot --space-id abc123 --catalog main --schema genie_functions --warehouse-id xyz789

  # Process only the first 10 conversations
  genie-trusted-asset-copilot --space-id abc123 --catalog main --schema genie_functions --max-conversations 10

  # Include moderate complexity queries
  genie-trusted-asset-copilot --space-id abc123 --catalog main --schema genie_functions --threshold moderate

  # Process conversations from the last 7 days
  genie-trusted-asset-copilot --space-id abc123 --catalog main --schema genie_functions --from 7d

  # Process conversations within a specific date range
  genie-trusted-asset-copilot --space-id abc123 --catalog main --schema genie_functions --from 2026-01-01 --to 2026-01-31

  # Process conversations since a specific date/time
  genie-trusted-asset-copilot --space-id abc123 --catalog main --schema genie_functions --from 2026-01-15T10:00:00
        """,
    )

    parser.add_argument(
        "--space-id",
        required=True,
        help="The Genie space ID to process.",
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog name for creating functions.",
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Schema name within the catalog for functions.",
    )
    parser.add_argument(
        "--warehouse-id",
        default=None,
        help="SQL warehouse ID for executing CREATE FUNCTION statements.",
    )
    parser.add_argument(
        "--max-conversations",
        type=int,
        default=None,
        help="Maximum number of conversations to process (default: all).",
    )
    parser.add_argument(
        "--include-all-users",
        action="store_true",
        help="Include conversations from all users (requires CAN MANAGE permission).",
    )
    parser.add_argument(
        "--model",
        default="databricks-claude-sonnet-4",
        help="Databricks model for complexity analysis (default: databricks-claude-sonnet-4).",
    )
    parser.add_argument(
        "--threshold",
        choices=["simple", "moderate", "complex"],
        default="complex",
        help="Minimum complexity threshold for creating assets (default: complex).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without applying them.",
    )
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Replace existing UC functions and SQL instructions instead of skipping.",
    )
    parser.add_argument(
        "--sql-instructions",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Create SQL example instructions in Genie room (default: enabled).",
    )
    parser.add_argument(
        "--uc-functions",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Create UC functions in Unity Catalog (default: enabled).",
    )
    parser.add_argument(
        "--register-functions",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Register UC functions with Genie room (default: enabled).",
    )
    parser.add_argument(
        "--optimize-sql",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Optimize SQL queries using sqlglot before creating assets (default: disabled).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose debug logging.",
    )
    parser.add_argument(
        "--from",
        dest="from_timestamp",
        type=str,
        default=None,
        help="Start of timestamp range (inclusive). Supports: relative (7d, 24h, 30m, 1w), ISO 8601 (2026-01-15T10:30:00), or date (2026-01-15).",
    )
    parser.add_argument(
        "--to",
        dest="to_timestamp",
        type=str,
        default=None,
        help="End of timestamp range (inclusive). Supports: relative (7d, 24h, 30m, 1w), ISO 8601 (2026-01-15T10:30:00), or date (2026-01-15).",
    )

    args = parser.parse_args()

    # Reconfigure logging if verbose
    if args.verbose:
        configure_logging(level="DEBUG")

    # Parse timestamp arguments if provided
    from_ts: int | None = None
    to_ts: int | None = None
    
    try:
        if args.from_timestamp:
            from_ts = parse_timestamp(args.from_timestamp)
            logger.info(
                f"Filtering conversations from: {datetime.fromtimestamp(from_ts / 1000, tz=timezone.utc).isoformat()}"
            )
        
        if args.to_timestamp:
            to_ts = parse_timestamp(args.to_timestamp)
            logger.info(
                f"Filtering conversations to: {datetime.fromtimestamp(to_ts / 1000, tz=timezone.utc).isoformat()}"
            )
    except ValueError as e:
        logger.error(f"Invalid timestamp format: {e}")
        return 1

    try:
        report = run(
            space_id=args.space_id,
            catalog=args.catalog,
            schema=args.schema,
            warehouse_id=args.warehouse_id,
            max_conversations=args.max_conversations,
            include_all_users=args.include_all_users,
            model=args.model,
            complexity_threshold=args.threshold,
            dry_run=args.dry_run,
            force=args.force,
            create_sql_instructions=args.sql_instructions,
            create_uc_functions=args.uc_functions,
            register_uc_functions=args.register_functions,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            optimize_sql=args.optimize_sql,
        )

        # Return non-zero if there were errors
        if report.errors:
            return 1
        return 0

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
