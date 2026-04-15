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
	.venv/Scripts/python -m pip install --upgrade pip
	.venv/Scripts/python -m pip install -r requirements.txt

start:
	.venv/Scripts/python src/index.py

tests:
	.venv/Scripts/python -m unittest discover -s test

clean:
	rm -rf .venv
	rm -rf __pycache__
	rm -rf .pytest_cache