# Convenience Makefile.

.PHONY: help deps develop test

help:
	@echo
	@echo "Helper for running the gnss-analysis project."
	@echo
	@echo "(Please read before using!)"
	@echo
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  help      to display this help message"
	@echo "  deps      to install dependencies"
	@echo "  develop   to build project for development"
	@echo "  test      to run all tests"
	@echo

deps:
	@echo
	@echo "Installing dependencies..."
	@echo
	pip install -r requirements.txt -e .
	@echo
	@echo "Finished!"

develop:
	@echo
	@echo "Building for development..."
	@echo
	sudo python setup.py develop
	@echo
	@echo "Finished!"

test:
	@echo
	@echo "Run Python tests..."
	@echo
	tox
	@echo
	@echo "Finished!"
