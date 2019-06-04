REQUIREMENTS="requirements-dev.txt"

all: test


flake8:
	@pip install flake8
	@flake8 cabbage tests


isort:
	@isort -rc cabbage -w 120
	@isort -rc tests -w 120


lint:
	@pip install pylint==2.2.2
	@pylint cabbage


install: uninstall
	pip install .
	@echo "Done"


uninstall:
	@pip uninstall cabbage -y


test:
	@py.test --cov=cabbage


clean:
	@rm -rf `find . -name __pycache__`
	@rm -f `find . -type f -name '*.py[co]' `
	@rm -f `find . -type f -name '*~' `
	@rm -f `find . -type f -name '.*~' `
	@rm -f `find . -type f -name '@*' `
	@rm -f `find . -type f -name '#*#' `
	@rm -f `find . -type f -name '*.orig' `
	@rm -f `find . -type f -name '*.rej' `
	@rm -f .coverage
	@rm -rf htmlcov
	@rm -rf build
	@rm -rf cover
	@python setup.py clean
	@rm -rf .tox
	@rm -f .flake
	@rm -rf .pytest_cache
	@rm -rf dist
	@rm -rf *.egg-info

install-dev: uninstall
	@pip install -Ur requirements-dev.txt
	@pip install -e .

.PHONY: all mypy isort install-dev uninstall clean test
