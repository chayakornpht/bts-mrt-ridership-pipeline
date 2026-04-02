.PHONY: up down restart logs dbt-run dbt-test dbt-docs clean

# =============================================
# Docker Commands
# =============================================

up:  ## Start all services
	docker-compose up -d
	@echo "✅ Services starting..."
	@echo "   Airflow UI:  http://localhost:8080  (admin/admin)"
	@echo "   Metabase:    http://localhost:3000"
	@echo "   Warehouse:   localhost:5434  (warehouse/warehouse123)"

down:  ## Stop all services
	docker-compose down

restart:  ## Restart all services
	docker-compose down && docker-compose up -d

logs:  ## View logs (all services)
	docker-compose logs -f

logs-airflow:  ## View Airflow logs only
	docker-compose logs -f airflow-webserver airflow-scheduler

# =============================================
# dbt Commands
# =============================================

dbt-run:  ## Run all dbt models
	cd dbt && dbt run --profiles-dir .

dbt-run-staging:  ## Run staging models only
	cd dbt && dbt run --select staging --profiles-dir .

dbt-run-marts:  ## Run mart models only
	cd dbt && dbt run --select marts --profiles-dir .

dbt-test:  ## Run all dbt tests
	cd dbt && dbt test --profiles-dir .

dbt-docs:  ## Generate and serve dbt docs
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .

# =============================================
# Data Extraction (manual run)
# =============================================

extract-bem:  ## Run BEM extraction manually
	python scripts/extract_bem_ridership.py

extract-bts:  ## Run BTS PDF extraction manually
	python scripts/extract_bts_pdf.py

extract-travel:  ## Run Google Maps extraction manually
	python scripts/extract_travel_times.py

# =============================================
# Setup
# =============================================

setup:  ## First-time setup
	cp .env.example .env
	pip install -r requirements.txt
	pip install dbt-postgres
	mkdir -p data/pdfs data/raw logs
	@echo "✅ Setup complete!"
	@echo "📝 Next steps:"
	@echo "   1. Edit .env with your API keys"
	@echo "   2. Download BTS PDFs to data/pdfs/"
	@echo "   3. Run 'make up' to start services"

clean:  ## Remove all containers and volumes
	docker-compose down -v
	rm -rf logs/ dbt/target/ dbt/dbt_packages/ dbt/logs/

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
