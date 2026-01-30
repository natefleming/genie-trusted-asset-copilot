"""
Conversation reader for extracting SQL queries from Genie conversations.

This module fetches conversations and messages from a Genie space,
extracting generated SQL queries from message attachments.
"""

import re
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import (
    GenieConversation,
    GenieMessage,
    MessageStatus,
)
from loguru import logger

from genie_trusted_asset_copilot.models import ExtractedQuery

# Message statuses that indicate successful SQL generation
SUCCESSFUL_STATUSES = {
    MessageStatus.COMPLETED,
    MessageStatus.EXECUTING_QUERY,  # Query is running, SQL was generated
}


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


class ConversationReader:
    """Reads conversations and extracts SQL queries from a Genie space."""

    def __init__(
        self,
        space_id: str,
        client: WorkspaceClient | None = None,
        include_all_users: bool = False,
        from_timestamp: int | None = None,
        to_timestamp: int | None = None,
    ) -> None:
        """
        Initialize the conversation reader.

        Args:
            space_id: The Genie space ID to read conversations from.
            client: Optional WorkspaceClient instance. If not provided, one will be created.
            include_all_users: If True, include conversations from all users
                (requires CAN MANAGE permission).
            from_timestamp: Optional start timestamp in milliseconds (inclusive). 
                Conversations created before this are excluded.
            to_timestamp: Optional end timestamp in milliseconds (inclusive). 
                Conversations created after this are excluded.
        """
        self.space_id = space_id
        self.client = client or WorkspaceClient()
        self.include_all_users = include_all_users
        self.from_timestamp = from_timestamp
        self.to_timestamp = to_timestamp

    def list_conversations(
        self,
        max_conversations: int | None = None,
    ) -> list[GenieConversation]:
        """
        List all conversations in the Genie space.

        Args:
            max_conversations: Maximum number of conversations to fetch in descending order by creation time (None for all).

        Returns:
            List of GenieConversation objects.
        """
        conversations: list[GenieConversation] = []
        page_token: str | None = None
        filtered_count = 0

        logger.info(f"Fetching conversations from Genie space: {self.space_id}")
        if self.from_timestamp:
            logger.info(
                f"Filtering from: {datetime.fromtimestamp(self.from_timestamp / 1000, tz=timezone.utc).isoformat()}"
            )
        if self.to_timestamp:
            logger.info(
                f"Filtering to: {datetime.fromtimestamp(self.to_timestamp / 1000, tz=timezone.utc).isoformat()}"
            )

        while True:
            response = self.client.genie.list_conversations(
                space_id=self.space_id,
                include_all=self.include_all_users,
                page_size=100,
                page_token=page_token,
            )

            if not response.conversations:
                break

            for conv in response.conversations:
                if max_conversations and len(conversations) >= max_conversations:
                    break
                
                # Filter by timestamp range if specified
                if self.from_timestamp is not None and conv.created_timestamp < self.from_timestamp:
                    filtered_count += 1
                    continue
                if self.to_timestamp is not None and conv.created_timestamp > self.to_timestamp:
                    filtered_count += 1
                    continue
                
                # Create a GenieConversation-like object from the summary
                conversations.append(conv)

            if max_conversations and len(conversations) >= max_conversations:
                break

            if response.next_page_token:
                page_token = response.next_page_token
            else:
                break

        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} conversations outside timestamp range")
        logger.info(f"Found {len(conversations)} conversations")
        return conversations

    def get_conversation_messages(
        self,
        conversation_id: str,
    ) -> list[GenieMessage]:
        """
        Get all messages for a conversation.

        Args:
            conversation_id: The conversation ID to fetch messages for.

        Returns:
            List of GenieMessage objects.
        """
        messages: list[GenieMessage] = []
        page_token: str | None = None

        while True:
            response = self.client.genie.list_conversation_messages(
                space_id=self.space_id,
                conversation_id=conversation_id,
                page_size=100,
                page_token=page_token,
            )

            if not response.messages:
                break

            messages.extend(response.messages)

            if response.next_page_token:
                page_token = response.next_page_token
            else:
                break

        return messages

    def get_message_with_sql(
        self,
        conversation_id: str,
        message_id: str,
    ) -> tuple[GenieMessage | None, str | None]:
        """
        Get a specific message with its SQL attachment.

        Args:
            conversation_id: The conversation ID.
            message_id: The message ID to fetch.

        Returns:
            Tuple of (GenieMessage, SQL string or None).
        """
        try:
            message = self.client.genie.get_message(
                space_id=self.space_id,
                conversation_id=conversation_id,
                message_id=message_id,
            )

            sql = self._extract_sql_from_message(message)
            return message, sql

        except Exception as e:
            logger.warning(f"Failed to get message {message_id}: {e}")
            return None, None

    def _is_successful_message(self, message: GenieMessage) -> bool:
        """
        Check if a message represents a successful Genie response.

        Args:
            message: The GenieMessage to check.

        Returns:
            True if the message status indicates success.
        """
        if message.status is None:
            # If no status, check if it has attachments with SQL (implies success)
            return bool(message.attachments)

        return message.status in SUCCESSFUL_STATUSES

    def _extract_sql_from_message(self, message: GenieMessage) -> str | None:
        """
        Extract SQL query from a message's attachments.

        Args:
            message: The GenieMessage to extract SQL from.

        Returns:
            The SQL query string, or None if no SQL was found.
        """
        if not message.attachments:
            return None

        for attachment in message.attachments:
            # The query.query field contains the SQL statement
            if attachment.query and attachment.query.query:
                return attachment.query.query

        return None

    def _extract_execution_time(self, message: GenieMessage) -> int | None:
        """
        Extract execution time from a message's query result.

        Args:
            message: The GenieMessage to extract execution time from.

        Returns:
            Execution time in milliseconds, or None if not available.
        """
        if not message.attachments:
            return None

        for attachment in message.attachments:
            if attachment.query:
                # Try to get execution time from query result metadata
                # The field name might vary based on SDK version
                query_obj = attachment.query
                # Check for common execution time field names
                for attr in ["execution_time_ms", "duration_ms", "elapsed_time_ms"]:
                    value = getattr(query_obj, attr, None)
                    if value is not None:
                        return int(value)

        return None

    def _normalize_question(self, question: str) -> str:
        """
        Normalize a question for deduplication comparison.

        Args:
            question: The question text to normalize.

        Returns:
            Normalized lowercase question without extra whitespace.
        """
        return " ".join(question.lower().split())

    def extract_all_queries(
        self,
        max_conversations: int | None = None,
    ) -> list[ExtractedQuery]:
        """
        Extract all SQL queries from conversations in the space.

        Deduplicates questions to avoid processing the same question multiple times.

        Args:
            max_conversations: Maximum number of conversations to process.

        Returns:
            List of ExtractedQuery objects containing questions and their SQL.
        """
        queries: list[ExtractedQuery] = []
        seen_questions: set[str] = set()  # Track normalized questions for deduplication
        conversations = self.list_conversations(max_conversations=max_conversations)
        total_messages = 0
        duplicates_skipped = 0

        for conv in conversations:
            conv_id = conv.conversation_id
            conv_title = conv.title or "Untitled"
            logger.debug(f"Processing conversation: {conv_id} - {conv_title[:50]}")

            messages = self.get_conversation_messages(conv_id)
            total_messages += len(messages)

            # Track user questions to pair with SQL responses
            last_user_question: str | None = None

            for i, msg in enumerate(messages):
                has_attachments = bool(msg.attachments)

                # If this message has content and no attachments, it's likely a user question
                if msg.content and not has_attachments:
                    last_user_question = msg.content
                    continue

                # Only process messages with successful status
                if not self._is_successful_message(msg):
                    logger.debug(
                        f"Skipping message with status {msg.status} "
                        f"(not successful)"
                    )
                    continue

                # Try to extract SQL directly from the message (attachments are already present)
                sql = self._extract_sql_from_message(msg)

                # If no SQL in the current message but we have an ID, try fetching full details
                if not sql and msg.id:
                    full_message, sql = self.get_message_with_sql(conv_id, msg.id)
                    if sql and full_message:
                        msg = full_message  # Use the full message for execution time

                if sql:
                    # Use the tracked user question, message content, or conversation title
                    question = last_user_question or msg.content or conv_title

                    # Deduplicate questions - skip if we've already seen this question
                    normalized_question = self._normalize_question(question)
                    if normalized_question in seen_questions:
                        logger.debug(f"Skipping duplicate question: {question[:60]}...")
                        duplicates_skipped += 1
                        last_user_question = None
                        continue

                    seen_questions.add(normalized_question)

                    execution_time = self._extract_execution_time(msg)
                    message_id = msg.id or f"{conv_id}_{i}"

                    queries.append(
                        ExtractedQuery(
                            question=question,
                            sql=sql,
                            execution_time_ms=execution_time,
                            message_id=message_id,
                            conversation_id=conv_id,
                        )
                    )
                    logger.debug(f"Extracted SQL for: {question[:60]}...")

                    # Reset user question after pairing
                    last_user_question = None

        if duplicates_skipped > 0:
            logger.info(f"Skipped {duplicates_skipped} duplicate questions")

        logger.info(
            f"Extracted {len(queries)} unique queries from "
            f"{len(conversations)} conversations ({total_messages} messages)"
        )
        return queries

    def _find_user_question(
        self,
        messages: list[GenieMessage],
        current_index: int,
        response_message: GenieMessage,
    ) -> str | None:
        """
        Find the user question that preceded a Genie response.

        Args:
            messages: List of all messages in the conversation.
            current_index: Index of the current message.
            response_message: The Genie response message.

        Returns:
            The user's question text, or None if not found.
        """
        # The response message might have the original question in its content
        # or we look at the preceding message
        if current_index > 0:
            prev_message = messages[current_index - 1]
            if prev_message.content:
                return prev_message.content

        # Fallback: check if the response has the question in a request field
        # or use the conversation title
        return response_message.content
