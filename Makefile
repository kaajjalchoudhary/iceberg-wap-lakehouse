.PHONY: install run report maintain test lint clean

PY ?= python
DAYS ?= 10

install:
	$(PY) -m pip install -r requirements.txt

run:        ## generate DAYS of meter files and push every batch through Write-Audit-Publish
	$(PY) -m meterlake run --days $(DAYS)

report:
	$(PY) -m meterlake report

maintain:   ## compaction, manifest rewrite, snapshot expiry
	$(PY) -m meterlake maintain

test:
	$(PY) -m pytest

lint:
	ruff check .

clean:
	rm -rf lake spark-warehouse metastore_db derby.log .pytest_cache
