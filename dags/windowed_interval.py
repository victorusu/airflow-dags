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
from airflow.hooks.base import BaseHook

from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


SERVER_DNS_NAME = "elastic-gbets-master"
ES_INDEX = "slurm-accounting-*"
ES_PAGE_SIZE = 10000

conn_str = BaseHook.get_connection(SERVER_DNS_NAME).get_uri()
def es_conn():
    return Elasticsearch([conn_str])

es_expected_items_number = 1000

def use_elasticsearch_hook(data_interval_start, data_interval_end):
    if not data_interval_start or not data_interval_end:
        return []

    es_conn = Elasticsearch([BaseHook.get_connection(SERVER_DNS_NAME).get_uri()])
    s = Search(using=es_conn, index=ES_INDEX) \
        .source(['jobid', 'cluster']) \
        .extra(track_total_hits=True, size=ES_PAGE_SIZE) \
        .filter('range',  **{'@end': {
            'gte': int(data_interval_start.timestamp()),
            'lt': int(data_interval_end.timestamp()),
            'format': 'epoch_second'
        }})

    print(s.to_dict())
    response = s.execute()
    assert response.success()
    print("hits ===> ", response.hits.total)
    assert response.hits.total.value < ES_PAGE_SIZE, "hits don't fit in page"
    jobs = [[int(j.jobid), (j.cluster if hasattr(j, 'cluster') else '')] for j in response]
    print(len(jobs), "jobs:", jobs)
    return jobs



@dag(
    schedule_interval=timedelta(minutes=5),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=5),
)
def windowed_interval():

    data_stream = "populate yours here"

    @task
    def get_raw_es_data(data_interval_start=None, data_interval_end=None):
        return use_elasticsearch_hook({"query": {"match_all": {}}})

    @task
    def operate_on_raw_data(loaded_jobs, data_interval_start=None, data_interval_end=None):
        """
        """
        print("check expectation ===> ", data_interval_start, data_interval_end)
        # columns = ['job_id', 'cluster']

        # es_jobs = pd.DataFrame(sorted(loaded_jobs), columns=columns)
        # print("jobs raws data ===> ", es_jobs)
        # counts = es_jobs.job_id.value_counts()
        # print("jobs raws data duplicated ===> ", es_jobs[es_jobs.job_id.isin(counts[counts > 1].index)])
        return True

    @task
    def check_es_data_consistency(check_expectation, data_interval_start=None, data_interval_end=None):
        if check_expectation:
            return True
        else:
            return False

    get_raw_es_data() >> operate_on_raw_data() >> check_es_data_consistency()


# Set the DAG to the one we just implemented
dag = windowed_interval()
