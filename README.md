# Airflow + Spark Parquet Compaction

Проект для автоматической генерации, компактификации Parquet файлов и записи метаданных через dag в Apache Airflow и Apache Spark.

## 🚀 Быстрый старт

### Предварительные требования
- Docker
- Docker Compose
- Make

### Запуск проекта
```bash
# Клонировать репозиторий
git clone https://github.com/zakaevislam/airflow_spark_compaction.git
cd airflow_spark_compaction

# Полная установка и запуск (одна команда)
make setup
```
После выполнения откройте Airflow UI (localhost:8080) (логин: airflow, пароль: airflow)

### Запуск отдельно спарк задания и полезные команды после

```bash
make build-spark-image
docker run -d --name spark_app spark-compaction-app:latest tail -f /dev/null
docker exec -it spark_app bash
export TARGET_SIZE_MB_PER_FILES=50
export DATA_PATH=./data
python3 spark_app.py
ls -l --block-size=M ./data/
```

### Структура проекта
```
airflow_spark_compaction/
├── airflow_deploy/          # Конфигурация Airflow
│   ├── dags/               # Airflow DAGs
│   ├── docker-compose.yml  # Docker Compose для Airflow
│   └── scripts/            # SQL скрипты для инициализации БД
├── spark_app/              # Spark приложение
│   ├── Dockerfile          # Образ Spark приложения
│   ├── spark_app.py        # Основное Spark приложение
│   └── requirements.txt    # Зависимости Python
├── Makefile                # Автоматизация команд
└── README.md              # Документация
```

### Команды Makefile
```
# Полная установка
make setup

# Запуск Airflow
deploy-airflow

# Сборка Spark образа
make build-spark-image

# Инициализация БД (создание таблиц)
make init-file-metadata-table

# Остановка всех сервисов
make clean
```