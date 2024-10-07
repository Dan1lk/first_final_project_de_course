from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, col, year, regexp_replace, mean, \
        unix_timestamp, from_unixtime, median, count, floor
from pyspark.sql.types import DoubleType, IntegerType, DateType
import clickhouse_connect
import zipfile
import urllib.request
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'main',
    default_args=default_args,
    description='A simple DAG to interact with ClickHouse and PostgreSQL without libraries',
    schedule_interval=None,
)

def pyspark_job(**context):

    spark = SparkSession.builder.appName('MySparkApp') \
        .master('local[*]') \
        .getOrCreate()

    # Скачаем файл и достаем данные из архива

    def extract_and_unpack():
        if not os.path.exists('file1.csv'):
            url = 'https://getfile.dokpub.com/yandex/get/https://disk.yandex.ru/d/bhf2M8C557AFVw'
            urllib.request.urlretrieve(url, filename="zip_file")

            with zipfile.ZipFile("zip_file", 'r') as zf:
                zf.extractall()

            with zipfile.ZipFile("Список домов РФ/archive (12).zip", 'r') as zf:
                zf.extractall()

            f = open('russian_houses.csv', mode='r', encoding='utf-16')
            with open('file1.csv', 'w', encoding='utf-8') as file:
                for i in f.readlines():
                    file.write(i)
            f.close()
            # Удаляем ненужные файлы и папки
            os.remove('Список домов РФ/archive (12).zip')
            os.rmdir('Список домов РФ')
            os.remove('zip_file')
            os.remove('russian_houses.csv')

    def transform_data():

        # Загрузка данных в dataframe
        houses_df = spark.read.csv('file1.csv', header=True, inferSchema=True)
        print(f'Общее количество строк: {houses_df.count()}')

        # Убираем пробелы в столбце square
        houses_df = houses_df.withColumn('square', regexp_replace(col('square'), "[\s]", ""))

        # Удаляем строки с пустыми ячейками
        houses_df = houses_df.na.drop("any")

        # Преобразуем столбцы в формат даты и чисел

        houses_df = houses_df.withColumn('maintenance_year', houses_df.maintenance_year.cast(DateType()))
        houses_df = houses_df.withColumn('square', houses_df.square.cast(DoubleType()))
        houses_df = houses_df.withColumn('population', houses_df.population.cast(IntegerType()))
        houses_df = houses_df.withColumn('communal_service_id', houses_df.communal_service_id.cast(DoubleType()))

        # Средний и медианный год постройки зданий:
        mean_and_median_maintenance_year_russian_houses = houses_df.agg(
            from_unixtime(mean(unix_timestamp(col('maintenance_year'))), 'y').alias('mean_maintenance_year'),
            from_unixtime(median(unix_timestamp(col('maintenance_year'))), 'y').alias('median_maintenance_year'))

        # Топ-10 областей и городов с наибольшим количеством объектов:
        top_10_regions_and_cities_with_the_largest_number_of_objects = houses_df.groupBy('region', 'locality_name').agg(
            count('address').alias('numbers_of_objects')) \
            .orderBy('numbers_of_objects', ascending=False).limit(10)

        # Здания с максимальной и минимальной площадью в рамках каждой области:
        buildings_with_max_and_min_square = houses_df.groupBy('region').agg(
            max(col("square")).alias('max_square'),
            min(col("square")).alias("min_square")
        ).orderBy('max_square', 'min_square', ascending=False)

        # Количество зданий по десятилетиям
        number_of_buildings_by_decade = houses_df.groupBy(
            (floor(year(col("maintenance_year")) / 10) * 10).alias('decade')) \
            .agg(count('*').alias('number_of_buildings')).orderBy('number_of_buildings', ascending=False)

        # Подключение к ClickHouse
        try:
            client = clickhouse_connect.get_client(host='clickhouse_user', port=8123)
            print("Connected to ClickHouse")

            # Вставка данных в ClickHouse
            client.insert_df(df=mean_and_median_maintenance_year_russian_houses.toPandas(),
                             table='mean_and_median_maintenance_year_russian_houses')
            client.insert_df(df=top_10_regions_and_cities_with_the_largest_number_of_objects.toPandas(),
                             table='top_10_regions_and_cities_with_the_largest_number_of_objects')
            client.insert_df(df=buildings_with_max_and_min_square.toPandas(), table='buildings_with_max_and_min_square')
            client.insert_df(df=number_of_buildings_by_decade.toPandas(), table='number_of_buildings_by_decade')

            print("The data is inserted")
        except Exception as e:
            print(f"Error connecting to ClickHouse: {e}")

        # Топ 25 домов, у которых площадь больше 60 кв.м

        houses_df.createOrReplaceTempView("russian_houses")

        top_25_houses_with_an_area_of_more_than_60_sq_m = spark.sql("""
        SELECT description, square
        FROM russian_houses rh
        WHERE square > 60.0
        ORDER BY square DESC
        LIMIT 25
        """)
        print("Топ 25 домов с площадью больше 60 кв.м")
        top_25_houses_with_an_area_of_more_than_60_sq_m.show(truncate=False)

    extract_and_unpack()
    transform_data()

    spark.stop()



task_query_pyspark = PythonOperator(
    task_id='query_pyspark',
    python_callable=pyspark_job,
    dag=dag
)

task_query_pyspark
