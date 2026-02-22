#! /bin/bash
pytest tests/ -v -s --log-cli-level=INFO --color=yes --timeout=900 2>&1
