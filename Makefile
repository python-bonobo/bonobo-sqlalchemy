# This file has been auto-generated.
# All changes will be lost, see Projectfile.
#
# Updated at 2016-12-29 19:04:33.277636

PYTHON ?= $(shell which python)
PYTHON_BASENAME ?= $(shell basename $(PYTHON))
PYTHON_REQUIREMENTS_FILE ?= requirements.txt
PYTHON_REQUIREMENTS_DEV_FILE ?= requirements-dev.txt
QUICK ?= 
VIRTUAL_ENV ?= .virtualenv-$(PYTHON_BASENAME)
PIP ?= $(VIRTUAL_ENV)/bin/pip
PYTEST ?= $(VIRTUAL_ENV)/bin/pytest
PYTEST_OPTIONS ?= --capture=no --cov=bonobo_sqlalchemy --cov-report html
YAPF ?= $(VIRTUAL_ENV)/bin/yapf
YAPF_OPTIONS ?= -rip
SPHINX_SOURCEDIR ?= docs

.PHONY: clean format install install-dev lint test

# Installs the local project dependencies.
install: $(VIRTUAL_ENV)
	if [ -z "$(QUICK)" ]; then \
	    $(PIP) install -U pip wheel -r $(PYTHON_REQUIREMENTS_FILE) ; \
	fi

# Installs the local project dependencies, including development-only libraries.
install-dev: $(VIRTUAL_ENV)
	if [ -z "$(QUICK)" ]; then \
	    $(PIP) install -U pip wheel -r $(PYTHON_REQUIREMENTS_DEV_FILE) ; \
	fi

# Cleans up the local mess.
clean:
	rm -rf build
	rm -rf dist

# Setup the local virtualenv, or use the one provided by the current environment.
$(VIRTUAL_ENV):
	virtualenv -p $(PYTHON) $(VIRTUAL_ENV)
	$(PIP) install -U pip wheel
	ln -fs $(VIRTUAL_ENV)/bin/activate activate-$(PYTHON_BASENAME)

lint: install-dev
	$(VIRTUAL_ENV)/bin/pylint --py3k bonobo_sqlalchemy -f html > pylint.html

test: install-dev
	$(PYTEST) $(PYTEST_OPTIONS) tests

format: install-dev
	$(YAPF) $(YAPF_OPTIONS) .
