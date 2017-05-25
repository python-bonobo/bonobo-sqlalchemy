# This file has been auto-generated.
# All changes will be lost, see Projectfile.
#
# Updated at 2017-05-25 16:58:39.980395

PACKAGE ?= bonobo_sqlalchemy
PYTHON ?= $(shell which python)
PYTHON_BASENAME ?= $(shell basename $(PYTHON))
PYTHON_DIRNAME ?= $(shell dirname $(PYTHON))
PYTHON_REQUIREMENTS_FILE ?= requirements.txt
PYTHON_REQUIREMENTS_DEV_FILE ?= requirements-dev.txt
QUICK ?= 
PIP ?= $(PYTHON_DIRNAME)/pip
PIP_INSTALL_OPTIONS ?= 
PYTEST ?= $(PYTHON_DIRNAME)/pytest
PYTEST_OPTIONS ?= --capture=no --cov=$(PACKAGE) --cov-report html
YAPF ?= $(PYTHON_DIRNAME)/yapf
YAPF_OPTIONS ?= -rip
SPHINX_SOURCEDIR ?= docs

.PHONY: clean format install install-dev lint test

# Installs the local project dependencies.
install:
	if [ -z "$(QUICK)" ]; then \
	    $(PIP) install -U pip wheel $(PYTHON_PIP_INSTALL_OPTIONS) -r $(PYTHON_REQUIREMENTS_FILE) ; \
	fi

# Installs the local project dependencies, including development-only libraries.
install-dev:
	if [ -z "$(QUICK)" ]; then \
	    $(PIP) install -U pip wheel $(PYTHON_PIP_INSTALL_OPTIONS) -r $(PYTHON_REQUIREMENTS_DEV_FILE) ; \
	fi

# Cleans up the local mess.
clean:
	rm -rf build dist *.egg-info

lint: install-dev
	$(PYTHON_DIRNAME)/pylint --py3k $(PACKAGE) -f html > pylint.html

test: install-dev
	$(PYTEST) $(PYTEST_OPTIONS) tests

format: install-dev
	$(YAPF) $(YAPF_OPTIONS) .
