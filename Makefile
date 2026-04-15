.PHONY: help cvenv venv install start clean

help:
	clear
	@echo "Makefile commands:"
	@echo "  venv      - Create a virtual environment"
	@echo "  install   - Install dependencies in the virtual environment"
	@echo "  start     - Start the application"
	@echo "  tests     - Run unit tests"
	@echo "  lint      - Run code linters"
	@echo "  format    - Format code using black"
	@echo "  clean     - Remove virtual environment and cache files"

venv:
	python3 -m venv .venv

install:
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

start:
	.venv/bin/python src/index.py

tests:
	.venv/bin/python -m unittest discover -s test

clean:
	rm -rf .venv
	rm -rf __pycache__
	rm -rf .pytest_cache