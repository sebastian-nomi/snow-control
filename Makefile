#!make
include .env
PYTHON_VERSION := 3.9
MODULE_DIR := $(CURDIR)/src

BREW_BIN := brew
PY_BIN := python$(PYTHON_VERSION)

ifneq ($(shell which $(BREW_BIN) >/dev/null 2>&1 && echo 0 || echo 1),0)
  $(error Could not find '$(BREW_BIN)'. Make sure it is installed and available on PATH)
endif

BREW_PACKAGES := pre-commit python@$(PYTHON_VERSION)

PRECOMMIT_MARKER := $(CURDIR)/.git/hooks/pre-commit

BREW_PREFIX := $(shell brew --prefix)
BREW_CELLAR := $(BREW_PREFIX)/Cellar

BREW_MARKERS := $(addprefix $(BREW_CELLAR)/,$(BREW_PACKAGES))

VENV_DIR := $(CURDIR)/venv
VENV_BIN_DIR := $(VENV_DIR)/bin
VENV_REQS := $(CURDIR)/requirements.txt

VENV_MARKER := $(VENV_BIN_DIR)/activate
REQS_MARKER := $(VENV_DIR)/requirements-installed

.PHONY: help # Taken from https://github.com/FalcoSuessgott/golang-cli-template/blob/master/Makefile
help: # Show this help
	@grep -E '^[a-zA-Z_-]+:.*?# .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?# "}; {printf "\033[1;32m%s\033[0m: %s\n", $$1, $$2}'

.PHONY: install
install: install_packages install_deps install_hooks $(CREDS_FILE) # Install all required tools and packages

.PHONY: install_packages
install_packages: $(BREW_MARKERS) # Install Brew packages

$(BREW_MARKERS):
	brew install "$(subst $(BREW_CELLAR)/,,$@)"

.PHONY: venv
venv: $(VENV_MARKER) # Create Python virtual environment


$(VENV_MARKER):
	$(PY_BIN) -m venv "$(VENV_DIR)" && $(VENV_BIN_DIR)/$(PY_BIN) -m pip install -U pip

.PHONY: install_deps
install_deps: venv $(REQS_MARKER) # Install Python dependencies

$(REQS_MARKER):
	$(VENV_BIN_DIR)/$(PY_BIN) -m pip install -r $(VENV_REQS) && \
	/usr/bin/touch $(REQS_MARKER)

.PHONY: install_hooks
install_hooks: install_deps $(PRECOMMIT_MARKER) # Install pre-commit git hooks

$(PRECOMMIT_MARKER):
	pre-commit install

.PHONY: shell
shell: install_deps $(CREDS_FILE) # Open new shell in a virtual environment
	@BASH_SILENCE_DEPRECATION_WARNING=1 \
	SNOWFLAKE_ORGANIZATION="$(SNOWFLAKE_ORGANIZATION)" \
	SNOWFLAKE_ACCOUNT="$(SNOWFLAKE_ACCOUNT)" \
	SNOWFLAKE_USER="$(SNOWFLAKE_USER)" \
	CONTROL_CONFIG_DIR="$(CONTROL_CONFIG_DIR)" \
	PYTHONPATH="$(MODULE_DIR):$$PYTHONPATH" \
	/bin/bash --init-file "$(VENV_MARKER)" -i


.PHONY: clean
clean: install # Remove all temporary files, artifacts and virtual environment
	@rm -rf $(VENV_DIR)
	@rm -rf $(CURDIR)/.ruff_cache
	@rm -rf $(CURDIR/)dist
	@rm -rf $(CURDIR)/snow_control.egg-info
	@rm -rf $(CURDIR)/src/snow_control.egg-info
	@find . -type d -name "__pycache__" -print0 | xargs -0 rm -rf
