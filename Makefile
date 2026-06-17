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

# extract:
# 	env $(shell cat .env | xargs) .venv/bin/python src/extraction.py

# transform:
# 	env $(shell cat .env | xargs) .venv/bin/python src/transformation.py

# gold:
# 	env $(shell cat .env | xargs) .venv/bin/python src/gold.py

# dashboard:
# 	env $(shell cat .env | xargs) .venv/bin/streamlit run src/dashboard.py

freeze:
	.venv/bin/pip freeze > requirements.txt

# PSQL=/Applications/Postgres.app/Contents/Versions/18/bin/psql

# db-create:
# 	$(PSQL) -U ryansiebert -f sql/001_create_database.sql

# db-migrate:
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -f sql/002_bronze_schema.sql

# db-setup: db-create db-migrate

# db-reset:
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -f sql/999_reset_bronze.sql
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -f sql/002_bronze_schema.sql

# db-reset-s:
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -f sql/999_reset_silver.sql
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -f sql/003_silver_schema.sql

# db-reset-g:
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -f sql/999_reset_gold.sql
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -f sql/004_gold_schema.sql
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -f sql/005_reporting_views.sql

# db-truncate:
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -c "TRUNCATE bronze.emission_factors, bronze.estimates RESTART IDENTITY;"

# db-truncate-s:
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -c "TRUNCATE silver.regions, silver.emission_factors, silver.estimates RESTART IDENTITY;"

# db-truncate-g:
# 	$(PSQL) -U ryansiebert -d climatiq_pipeline -c "TRUNCATE gold.emission_factors_fact, gold.year_dim, gold.sector_dim, gold.region_dim RESTART IDENTITY CASCADE;"
