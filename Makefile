SHELL:=/bin/bash

PYTHON ?= python
CYTHON ?= cython
PYTEST ?= pytest
CTAGS ?= ctags
SQLITE ?= sqlite3
DB_HOME ?= ${HOME}/catserv.db

catserv.db:
	$(SQLITE) ${HOME}/catserv.db

clean:
	$(PYTHON) setup.py clean
	rm -rf dist

in: inplace # just a shortcut
inplace:
	$(PYTHON) setup.py build_ext -i

test-code: in
	$(PYTEST) --showlocals -v tests/ --durations=20

test: test-code

# Python Environment management
.PHONY: *-environment
create-environment:
	conda env create --force -f environment.yml

update-environment:
	conda env update --file environment.yml  --prune

list-outdated-packages:
	pip list --outdated --format=columns
