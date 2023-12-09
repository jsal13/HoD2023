set shell := ["zsh", "-cu"]

default:
  just --list

docs-serve:
  mkdocs serve

docs-build:
  mkdocs build

env: poetry-install
  poetry shell 

poetry-install:
  poetry install

test:
  pytest --doctest-modules
