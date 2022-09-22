import datetime
import pendulum
import requests
import os

# from datetime import datetime, timedelta
# from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

from elasticsearch.client import Elasticsearch

from airflow.decorators import dag, task
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# Operators; we need this to operate!
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.operators.bash import BashOperator
# from airflow.providers.elasticsearch.hooks.elasticsearch

from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


def use_elasticsearch_hook(query_dict):
    # es_hook = ElasticsearchPythonHook(hosts=["https://elastic-gbets-master:9200"])
    # query = {"query": {"match_all": {}}}
    # self.es_result = es_hook.search(query=query)
    # return es_hook.search(query=query_dict)
    return []


def check_es_data_validity(es_data):
    return True


def operate_on_es_data(es_data):
    pass


def send_data_back_to_es(es_data, data_stream_name):
    pass


@dag(
    schedule_interval=timedelta(minutes=5),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=5),
)
def scheduled_interval():

    es_data = None
    data_stream = "populate yours here"

    @task
    def get_raw_es_data():
        es_data = use_elasticsearch_hook({"query": {"match_all": {}}})
        return 0

    @task
    def check_es_data_consistency():
        # TODO: check if the data is valid
        if check_es_data_validity(es_data):
            operate_on_es_data(es_data)
            send_data_back_to_es(es_data, data_stream)
            return 0
        else:
            return 1

    @task
    def send_data_to_es():
        send_data_back_to_es(es_data, data_stream)

    get_raw_es_data() >> check_es_data_consistency() >> send_data_to_es()


# Set the DAG to the one we just implemented
dag = scheduled_interval()
