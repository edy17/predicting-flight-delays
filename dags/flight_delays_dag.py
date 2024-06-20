import datetime as dt
import os
import re
from io import BytesIO
from typing import Sequence
from zipfile import ZipFile

import pandas as pd
import requests
from airflow.models import Variable, Connection
from airflow.models.dag import DAG
from airflow.models.param import ParamsDict
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


class CustomPythonOperator(PythonOperator):
    template_fields: Sequence[str] = ("processed_year",)

    def __init__(self, processed_year: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.processed_year = processed_year

    def execute(self, context) -> None:
        if not bool(context.get("params")):
            context.update({"params": ParamsDict({"year": self.processed_year})})
        super().execute(context)


class CustomSparkSubmitOperator(SparkSubmitOperator):
    template_fields: Sequence[str] = ("processed_year", "application_args",)

    def __init__(self, processed_year: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.processed_year = processed_year

    def execute(self, context) -> None:
        if not bool(context.get("params")):
            context.update({"params": ParamsDict({"year": self.processed_year})})
        super().execute(context)


with DAG("flight_delays_modeling_dag",
         schedule=None,
         start_date=dt.datetime(2024, 6, 1),
         is_paused_upon_creation=False,
         ) as dag:
    bts_dataset_url = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_"
    ncei_dataset_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access"
    station_per_airport_file = f"{os.environ['AIRFLOW_HOME']}/wban_airport_timezone.csv"
    bucket_name = "predicting-flight-delays"
    raw_data_location = f"s3a://{bucket_name}/task"
    lakehouse_location = f"s3a://{bucket_name}/spark-warehouse"
    models_location = f"s3a://{bucket_name}/models"
    spark_jars_packages = "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.1"
    application_path = f"file://{os.environ['AIRFLOW_HOME']}/target/scala-2.12/predicting-flight-delays_2.12-0.1.0-SNAPSHOT.jar"
    spark_conn_id = "spark_cluster"
    aws_conn_id = "aws_conn"
    aws_conn = Connection.get_connection_from_secrets(aws_conn_id)
    aws_conf = {"spark.hadoop.fs.s3a.access.key": aws_conn.login,
                "spark.hadoop.fs.s3a.secret.key": aws_conn.password}


    def get_processed_year():
        max_year = 2024
        try:
            year = int(Variable.get("year")) + 1  # Value of current run
        except:
            year = 2012  # Value of first run
        if year > max_year:
            raise Exception("All years are already processed.")
        Variable.set(key="year", value=year)
        return year


    def flight_data_ingestion(params):
        print("::group:: flight_data_ingestion")
        year = params["year"]
        hook = S3Hook(aws_conn_id)
        processed_months = range(1, 13)
        for month in processed_months:
            url = f"{bts_dataset_url}{year}_{month}.zip"
            response = requests.get(url, stream=True, allow_redirects=True)
            chunk_size = 1024
            buffer = BytesIO()
            for data in response.iter_content(chunk_size=chunk_size):
                buffer.write(data)
            with ZipFile(buffer) as zip_file:
                for file in zip_file.namelist():
                    if file.endswith(".csv"):
                        content = zip_file.read(file)
                        key = f"task/{year}/flights/{file}"
                        hook.load_bytes(content, key, bucket_name)
                        if hook.check_for_key(key, bucket_name):
                            print(f"{key} file download done")
        print("::endgroup::")


    def weather_data_ingestion(params):
        print("::group:: weather_data_ingestion")
        year = params["year"]
        url = f"{ncei_dataset_url}/{year}"
        response = requests.get(url, stream=True, allow_redirects=True)
        station_files = re.findall("href=\"(.*?csv)\"", response.text)
        station_ids = pd.read_csv(station_per_airport_file)["WBAN"].astype(str).tolist()
        known_stations = list(map(lambda x: (x[len(x) - 5 - 4:len(x) - 4], x), station_files))
        known_stations = list(filter(lambda x: x[0] in station_ids, known_stations))
        known_stations = list(map(lambda x: x[1], known_stations))
        hook = S3Hook(aws_conn_id)
        for file_name in known_stations:
            weather_url = f"{ncei_dataset_url}/{year}/{file_name}"
            key = f"task/{year}/weather/{file_name}"
            response = requests.get(weather_url, stream=True, allow_redirects=True)
            hook.load_bytes(response.content, key, bucket_name)
            if hook.check_for_key(key, bucket_name):
                print(f"{key} file download done")
        print("::endgroup::")


    get_current_year = PythonOperator(
        task_id="get_processed_year",
        python_callable=get_processed_year)

    current_year = "{{ ti.xcom_pull(task_ids='get_processed_year', dag_id='flight_delays_modeling_dag', key='return_value') }}"
    year_param = "{{ params['year'] }}"

    download_flights = CustomPythonOperator(
        task_id="flight_data_ingestion",
        python_callable=flight_data_ingestion,
        processed_year=current_year)

    download_weather = CustomPythonOperator(
        task_id="weather_data_ingestion",
        python_callable=weather_data_ingestion,
        processed_year=current_year)

    flights_preprocessing = CustomSparkSubmitOperator(
        task_id="flight_data_preprocessing",
        name="flight_data_preprocessing",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.FlightsPreprocessing",
        application=application_path,
        application_args=[f"file://{station_per_airport_file}",
                          f"{raw_data_location}/{year_param}/flights",
                          f"{lakehouse_location}/{year_param}/flights"],
        processed_year=current_year)

    weather_preprocessing = CustomSparkSubmitOperator(
        task_id="weather_data_preprocessing",
        name="weather_data_preprocessing",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.WeatherPreprocessing",
        application=application_path,
        application_args=[f"file://{station_per_airport_file}",
                          f"{raw_data_location}/{year_param}/weather",
                          f"{lakehouse_location}/{year_param}/raw_weather"],
        processed_year=current_year)

    weather_resampling = CustomSparkSubmitOperator(
        task_id="weather_data_resampling",
        name="weather_data_resampling",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.WeatherResampling",
        application=application_path,
        application_args=[
            f"{lakehouse_location}/{year_param}/raw_weather",
            f"{lakehouse_location}/{year_param}/weather"],
        processed_year=current_year)

    weather_interpolation = CustomSparkSubmitOperator(
        task_id="weather_data_interpolation",
        name="weather_data_interpolation",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.WeatherInterpolation",
        application=application_path,
        application_args=[
            f"{lakehouse_location}/{year_param}/weather",
            f"{lakehouse_location}/{year_param}/interpolated_weather"],
        processed_year=current_year)

    weather_assembling = CustomSparkSubmitOperator(
        task_id="weather_data_assembling",
        name="weather_data_assembling",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.WeatherAssembling",
        application=application_path,
        application_args=[
            f"{lakehouse_location}/{year_param}/interpolated_weather",
            f"{lakehouse_location}/{year_param}/org_vectorized_weather",
            f"{lakehouse_location}/{year_param}/dest_vectorized_weather"],
        processed_year=current_year)

    origin_airports_processing = CustomSparkSubmitOperator(
        task_id="origin_airports_processing",
        name="origin_airports_processing",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.OriginAirportsProcessing",
        application=application_path,
        application_args=[
            f"{lakehouse_location}/{year_param}/flights",
            f"{lakehouse_location}/{year_param}/org_vectorized_weather",
            f"{lakehouse_location}/{year_param}/org_delay"],
        processed_year=current_year)

    destination_airports_processing = CustomSparkSubmitOperator(
        task_id="destination_airports_processing",
        name="destination_airports_processing",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.DestinationAirportsProcessing",
        application=application_path,
        application_args=[
            f"{lakehouse_location}/{year_param}/org_delay",
            f"{lakehouse_location}/{year_param}/dest_vectorized_weather",
            f"{lakehouse_location}/{year_param}/org_dest_delay"],
        processed_year=current_year)

    vector_assembling = CustomSparkSubmitOperator(
        task_id="vector_assembling",
        name="vector_assembling",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.VectorAssembling",
        application=application_path,
        application_args=[
            f"{lakehouse_location}/{year_param}/org_dest_delay",
            f"{lakehouse_location}/{year_param}/delay"],
        processed_year=current_year)

    under_sampling = CustomSparkSubmitOperator(
        task_id="data_under_sampling",
        name="data_under_sampling",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.UnderSampling",
        application=application_path,
        application_args=[
            f"{lakehouse_location}/{year_param}/delay",
            f"{lakehouse_location}/train_delay",
            f"{lakehouse_location}/test_delay"],
        processed_year=current_year)

    modeling = CustomSparkSubmitOperator(
        task_id="model_training_and_evaluation",
        name="model_training_and_evaluation",
        packages=spark_jars_packages,
        conf=aws_conf,
        conn_id=spark_conn_id,
        java_class="org.diehl.workflow.Modeling",
        application=application_path,
        application_args=[f"{lakehouse_location}/train_delay",
                          f"{lakehouse_location}/test_delay",
                          f"{lakehouse_location}/{year_param}/train_roc",
                          f"{lakehouse_location}/{year_param}/test_roc"
                          f"{models_location}/{year_param}"],
        processed_year=current_year)

    get_current_year >> [download_flights, download_weather]
    download_flights >> flights_preprocessing
    download_weather >> weather_preprocessing
    weather_preprocessing >> weather_resampling
    weather_resampling >> weather_interpolation
    weather_interpolation >> weather_assembling
    [flights_preprocessing, weather_assembling] >> origin_airports_processing
    origin_airports_processing >> destination_airports_processing
    destination_airports_processing >> vector_assembling
    vector_assembling >> under_sampling
    under_sampling >> modeling
