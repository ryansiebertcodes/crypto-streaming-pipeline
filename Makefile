.venv:
	python3 -m venv .venv

install: .venv
	.venv/bin/pip install -r requirements.txt

producer:
	env $(shell cat .env | xargs) .venv/bin/python src/producer.py

stream:
	env $(shell cat .env | xargs) JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python src/spark_job.py

stream-fresh:
	rm -rf checkpoints/raw-trades
	env $(shell cat .env | xargs) JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python src/spark_job.py

dbt-run:
	cd crypto_stream && dbt run

dbt-test:
	cd crypto_stream && dbt test

dbt-all: dbt-run dbt-test

freeze:
	.venv/bin/pip freeze > requirements.txt
