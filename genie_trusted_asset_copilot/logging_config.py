"""
Logging configuration for Genie Trusted Asset Copilot.

This module configures loguru for stdout-only logging with no file output.
It also suppresses noisy third-party library logs.
"""

import logging
import sys

from loguru import logger


def configure_logging(level: str = "INFO") -> None:
    """
    Configure loguru for stdout-only logging.

    Args:
        level: The minimum log level to display (default: INFO).
    """
    # Remove default handler
    logger.remove()

    # Add stdout handler with custom format
    logger.add(
        sys.stdout,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        level=level,
        colorize=True,
    )

    # Suppress noisy third-party library logs
    _suppress_third_party_logs()


def _suppress_third_party_logs() -> None:
    """Suppress noisy log messages from third-party libraries."""
    # Suppress Databricks SDK authentication logs
    logging.getLogger("databricks.sdk").setLevel(logging.WARNING)

    # Suppress MLflow tracing warnings
    logging.getLogger("mlflow").setLevel(logging.ERROR)
    logging.getLogger("mlflow.tracing").setLevel(logging.ERROR)

    # Suppress httpx/httpcore logs (used by Databricks SDK)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # Suppress LangChain verbose logs
    logging.getLogger("langchain").setLevel(logging.WARNING)
    logging.getLogger("langchain_core").setLevel(logging.WARNING)
