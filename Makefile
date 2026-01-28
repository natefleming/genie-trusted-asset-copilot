TOP_DIR := .
SRC_DIR := $(TOP_DIR)/genie_trusted_asset_copilot
LIB_NAME := genie-trusted-asset-copilot
LIB_VERSION := $(shell grep -m 1 version pyproject.toml | tr -s ' ' | tr -d '"' | tr -d "'" | cut -d' ' -f3)

UV := uv
SYNC := $(UV) sync
PYTHON := $(UV) run python
RUFF_CHECK := $(UV) run ruff check --fix --ignore E501
RUFF_FORMAT := $(UV) run ruff format
FIND := $(shell which find)
RM := rm -rf

# Required configuration
GENIE_SPACE_ID ?=
CATALOG ?=
SCHEMA ?=

# Optional configuration
WAREHOUSE_ID ?=
MAX_CONVERSATIONS ?=
THRESHOLD ?= complex
MODEL ?= databricks-claude-sonnet-4

.PHONY: all install depends check format clean distclean analyze create-assets analyze-quick help

all: install

install: depends
	$(SYNC)

depends:
	@$(SYNC)

check:
	$(RUFF_CHECK) $(SRC_DIR)

format: check
	$(RUFF_FORMAT) $(SRC_DIR)

clean:
	$(FIND) $(SRC_DIR) -name \*.pyc -exec rm -f {} \; 2>/dev/null || true
	$(FIND) $(SRC_DIR) -name \*.pyo -exec rm -f {} \; 2>/dev/null || true

distclean: clean
	$(RM) $(TOP_DIR)/.mypy_cache
	$(RM) $(TOP_DIR)/.ruff_cache
	$(FIND) $(SRC_DIR) \( -name __pycache__ -a -type d \) -prune -exec rm -rf {} \; 2>/dev/null || true

# Analyze Genie conversations (dry-run mode)
# Usage: make analyze GENIE_SPACE_ID=<space-id> CATALOG=<catalog> SCHEMA=<schema>
analyze:
ifndef GENIE_SPACE_ID
	$(error GENIE_SPACE_ID is required. Usage: make analyze GENIE_SPACE_ID=<space-id> CATALOG=<catalog> SCHEMA=<schema>)
endif
ifndef CATALOG
	$(error CATALOG is required. Usage: make analyze GENIE_SPACE_ID=<space-id> CATALOG=<catalog> SCHEMA=<schema>)
endif
ifndef SCHEMA
	$(error SCHEMA is required. Usage: make analyze GENIE_SPACE_ID=<space-id> CATALOG=<catalog> SCHEMA=<schema>)
endif
	$(UV) run genie-trusted-asset-copilot \
		--space-id $(GENIE_SPACE_ID) \
		--catalog $(CATALOG) \
		--schema $(SCHEMA) \
		--threshold $(THRESHOLD) \
		--model $(MODEL) \
		$(if $(MAX_CONVERSATIONS),--max-conversations $(MAX_CONVERSATIONS),) \
		--dry-run

# Create trusted assets and UC functions
# Usage: make create-assets GENIE_SPACE_ID=<space-id> CATALOG=<catalog> SCHEMA=<schema> [WAREHOUSE_ID=<warehouse-id>]
create-assets:
ifndef GENIE_SPACE_ID
	$(error GENIE_SPACE_ID is required)
endif
ifndef CATALOG
	$(error CATALOG is required)
endif
ifndef SCHEMA
	$(error SCHEMA is required)
endif
	$(UV) run genie-trusted-asset-copilot \
		--space-id $(GENIE_SPACE_ID) \
		--catalog $(CATALOG) \
		--schema $(SCHEMA) \
		--threshold $(THRESHOLD) \
		--model $(MODEL) \
		$(if $(WAREHOUSE_ID),--warehouse-id $(WAREHOUSE_ID),) \
		$(if $(MAX_CONVERSATIONS),--max-conversations $(MAX_CONVERSATIONS),)

# Quick analysis with limited conversations (dry-run)
# Usage: make analyze-quick GENIE_SPACE_ID=<space-id> CATALOG=<catalog> SCHEMA=<schema>
analyze-quick:
ifndef GENIE_SPACE_ID
	$(error GENIE_SPACE_ID is required)
endif
ifndef CATALOG
	$(error CATALOG is required)
endif
ifndef SCHEMA
	$(error SCHEMA is required)
endif
	$(UV) run genie-trusted-asset-copilot \
		--space-id $(GENIE_SPACE_ID) \
		--catalog $(CATALOG) \
		--schema $(SCHEMA) \
		--threshold $(THRESHOLD) \
		--max-conversations 5 \
		--dry-run \
		--verbose

help:
	$(info $(LIB_NAME) v$(LIB_VERSION))
	$(info )
	$(info $$> make [all|install|depends|check|format|clean|distclean|analyze|create-assets|analyze-quick|help])
	$(info )
	$(info   Setup:)
	$(info       all          - install dependencies (default))
	$(info       install      - install dependencies)
	$(info       depends      - sync dependencies)
	$(info )
	$(info   Code Quality:)
	$(info       check        - run ruff linter with auto-fix)
	$(info       format       - format source code with ruff)
	$(info )
	$(info   Cleanup:)
	$(info       clean        - remove .pyc/.pyo files)
	$(info       distclean    - remove all build artifacts and caches)
	$(info )
	$(info   Trusted Asset Copilot:)
	$(info       analyze      - analyze conversations and preview trusted assets (dry-run))
	$(info                      Required: GENIE_SPACE_ID, CATALOG, SCHEMA)
	$(info                      Optional: THRESHOLD=complex, MAX_CONVERSATIONS, MODEL)
	$(info )
	$(info       create-assets - create trusted assets and UC functions)
	$(info                      Required: GENIE_SPACE_ID, CATALOG, SCHEMA)
	$(info                      Optional: WAREHOUSE_ID (for UC functions), THRESHOLD, MAX_CONVERSATIONS)
	$(info )
	$(info       analyze-quick - quick test with 5 conversations (dry-run, verbose))
	$(info                      Required: GENIE_SPACE_ID, CATALOG, SCHEMA)
	$(info )
	$(info   Configuration Variables:)
	$(info       GENIE_SPACE_ID     - Genie space ID (required))
	$(info       CATALOG            - Unity Catalog name (required))
	$(info       SCHEMA             - Schema name (required))
	$(info       WAREHOUSE_ID       - SQL warehouse ID (optional, for UC functions))
	$(info       THRESHOLD          - Complexity threshold: simple|moderate|complex (default: complex))
	$(info       MAX_CONVERSATIONS  - Limit conversations to process (optional))
	$(info       MODEL              - LLM model for analysis (default: databricks-claude-sonnet-4))
	$(info )
	$(info   Examples:)
	$(info       make analyze GENIE_SPACE_ID=abc123 CATALOG=main SCHEMA=genie_functions)
	$(info       make create-assets GENIE_SPACE_ID=abc123 CATALOG=main SCHEMA=genie_functions WAREHOUSE_ID=xyz789)
	$(info       make analyze-quick GENIE_SPACE_ID=abc123 CATALOG=main SCHEMA=genie_functions)
	$(info       make analyze GENIE_SPACE_ID=abc123 CATALOG=main SCHEMA=genie_functions THRESHOLD=moderate)
	$(info )
	@true
