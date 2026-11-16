FROM apache/airflow:3.2.2-python3.12

COPY requirements-airflow.txt /requirements-airflow.txt
# Se fija apache-airflow para que pip no lo actualice al instalar las dependencias
RUN pip install --no-cache-dir "apache-airflow==3.2.2" -r /requirements-airflow.txt
