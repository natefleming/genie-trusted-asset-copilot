"""
Data models for Genie Trusted Asset Copilot.

All models use Pydantic for strong typing and validation.
"""

from enum import Enum

from pydantic import BaseModel, Field


class SQLComplexity(str, Enum):
    """Classification of SQL query complexity."""

    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"


class ExtractedQuery(BaseModel):
    """A SQL query extracted from a Genie conversation message."""

    question: str = Field(description="The user's original question")
    sql: str = Field(description="The generated SQL query")
    execution_time_ms: int | None = Field(
        default=None, description="Query execution time in milliseconds"
    )
    message_id: str = Field(description="The Genie message ID")
    conversation_id: str = Field(description="The Genie conversation ID")


class SQLParameter(BaseModel):
    """A parameter extracted from a SQL query that can be made variable."""

    name: str = Field(description="Parameter name (snake_case, e.g. 'start_date')")
    sql_type: str = Field(
        description="Genie parameter type: String, Date, Date and Time, Decimal, or Integer"
    )
    original_value: str = Field(description="The original literal value in the SQL")
    description: str = Field(description="Human-readable description of what this parameter represents")
    default_value: str | None = Field(
        default=None, description="Optional default value for the parameter"
    )


class ParameterExtraction(BaseModel):
    """LLM-generated extraction of parameters from a SQL query."""

    parameters: list[SQLParameter] = Field(
        default_factory=list,
        description="List of extracted parameters that can be made variable",
    )
    parameterized_sql: str = Field(
        description="SQL with literal values replaced by parameter placeholders"
    )
    reasoning: str = Field(
        description="Explanation of which values were parameterized and why"
    )


class ComplexityAnalysis(BaseModel):
    """LLM-generated analysis of SQL query complexity."""

    complexity: SQLComplexity = Field(description="Overall complexity classification")
    reasoning: str = Field(description="Explanation of the complexity assessment")
    has_joins: bool = Field(default=False, description="Contains JOIN operations")
    has_subqueries: bool = Field(default=False, description="Contains subqueries")
    has_ctes: bool = Field(default=False, description="Contains CTEs (WITH clauses)")
    has_window_functions: bool = Field(
        default=False, description="Contains window functions (OVER, PARTITION BY)"
    )
    has_aggregations: bool = Field(
        default=False, description="Contains GROUP BY or aggregate functions"
    )
    join_count: int = Field(default=0, description="Number of JOIN operations")


class TrustedAssetCandidate(BaseModel):
    """A candidate for promotion to a Genie trusted asset."""

    question: str = Field(description="The user's original question")
    sql: str = Field(description="The original SQL query")
    complexity: ComplexityAnalysis = Field(description="Complexity analysis results")
    execution_time_ms: int | None = Field(
        default=None, description="Query execution time in milliseconds"
    )
    message_id: str = Field(description="Source message ID for traceability")
    conversation_id: str = Field(description="Source conversation ID for traceability")
    parameters: list[SQLParameter] = Field(
        default_factory=list, description="Extracted parameters for the query"
    )
    parameterized_sql: str | None = Field(
        default=None, description="SQL with parameter placeholders (if parameters extracted)"
    )


class QueryParameter(BaseModel):
    """A parameter definition for a Genie example SQL query."""

    name: str = Field(description="The parameter name used in SQL (e.g., 'site_name')")
    type_hint: str = Field(
        default="STRING",
        description="Parameter type hint: STRING, DATE, TIMESTAMP, DECIMAL, or INTEGER",
    )


class ExampleQuestionSQL(BaseModel):
    """Structure for a Genie space example_question_sqls entry."""

    id: str = Field(description="Unique identifier for the example")
    question: list[str] = Field(description="The example question text")
    sql: list[str] = Field(description="The SQL query lines")
    usage_guidance: list[str] | None = Field(
        default=None,
        description="Additional context explaining when this query is relevant (as array)",
    )
    parameters: list[QueryParameter] | None = Field(
        default=None,
        description="Parameter definitions with name, displayName, and type",
    )


class SqlFunction(BaseModel):
    """Structure for a Genie space sql_functions entry."""

    id: str = Field(description="Unique identifier (32-hex UUID without hyphens)")
    identifier: str = Field(
        description="Full function path (catalog.schema.function_name)"
    )


class GenieSpaceInstructions(BaseModel):
    """Instructions section of a Genie space configuration."""

    text_instructions: list[dict] = Field(default_factory=list)
    example_question_sqls: list[ExampleQuestionSQL] = Field(default_factory=list)
    sql_functions: list[SqlFunction] = Field(default_factory=list)
    join_specs: list[dict] = Field(default_factory=list)
    sql_snippets: dict = Field(default_factory=dict)


class GenieSpaceConfig(BaseModel):
    """Configuration structure for a Genie space (serialized_space)."""

    version: int = Field(default=1)
    config: dict = Field(default_factory=dict)
    data_sources: dict = Field(default_factory=dict)
    instructions: GenieSpaceInstructions = Field(
        default_factory=GenieSpaceInstructions
    )


class CreationResult(BaseModel):
    """Result of creating a trusted asset or UC function."""

    success: bool = Field(description="Whether creation was successful")
    asset_type: str = Field(description="Type of asset created (trusted_asset or uc_function)")
    name: str = Field(description="Name or identifier of the created asset")
    error: str | None = Field(default=None, description="Error message if creation failed")


class ProcessingReport(BaseModel):
    """Summary report of the trusted asset creation process."""

    total_conversations: int = Field(description="Total conversations processed")
    total_messages: int = Field(description="Total messages processed")
    queries_extracted: int = Field(description="Number of SQL queries extracted")
    complex_queries: int = Field(description="Number of queries classified as complex")
    trusted_assets_created: int = Field(description="Number of trusted assets created")
    uc_functions_created: int = Field(description="Number of UC functions created")
    uc_functions_registered: int = Field(
        default=0, description="Number of UC functions registered with Genie room"
    )
    errors: list[str] = Field(default_factory=list, description="List of errors encountered")
