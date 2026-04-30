from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Varsayılan Ayarlar
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2, # Hata alırsak 2 kere daha deneyecek
    'retry_delay': timedelta(minutes=2),
}

# 2. DAG Tanımlaması
with DAG(
    'nyc_taxi_production_etl',
    default_args=default_args,
    description='Uçtan Uca NYC Taksi ETL Akışı',
    schedule_interval='@daily', # Her gece yarısı çalışacak
    catchup=False,
    tags=['spark', 'minio', 'etl']
) as dag:

    # 3. Görevler (Tasks)
    
    # Başlangıç ve Bitiş noktaları (Görsel akışı netleştirmek için kullanılır)
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # Verinin landing bucket'ında olup olmadığını kontrol eden simülasyon adımı
    check_data_readiness = BashOperator(
        task_id='check_data_readiness',
        bash_command='echo "Landing zone kontrol ediliyor..."'
    )

    # Spark ETL işini tetiklediğimiz ana adım
    trigger_spark_etl = BashOperator(
        task_id='trigger_spark_etl_job',
        bash_command='echo "Spark kümesine tetik gönderildi: taxi_etl_job.py çalıştırılıyor..." && sleep 5 && echo "İşlem tamamlandı"'
    )

    # İşlem bitince takıma Slack/Email üzerinden bildirim atacak adım
    notify_team = BashOperator(
        task_id='send_success_notification',
        bash_command='echo "..................."'
    )

    # 4. Bağımlılıklar ve Akış (Dependencies)
    start_pipeline >> check_data_readiness >> trigger_spark_etl >> notify_team >> end_pipeline
