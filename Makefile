.PHONY: deploy-airflow build-spark-image init-file-metadata-table clean

setup: deploy-airflow init-file-metadata-table build-spark-image
	@echo "All services are ready!"

deploy-airflow:
	cd airflow_deploy && docker-compose up -d

build-spark-image:
	docker build -t spark-compaction-app ./spark_app

init-file-metadata-table:
	cd airflow_deploy && docker-compose exec postgres psql -U airflow -d airflow -c "\
    CREATE TABLE IF NOT EXISTS file_metadata (\
            id SERIAL PRIMARY KEY,\
            data_path TEXT NOT NULL,\
            number_of_files INTEGER NOT NULL,\
            average_files_size DECIMAL(10,2) NOT NULL,\
            dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP\
    );"

clean:
	cd airflow_deploy && docker-compose down