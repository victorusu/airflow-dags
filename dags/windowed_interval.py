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

# def create_es_client():
#     # Basic Elasticsearch info
#     ES_HOST = os.environ.get("ES_HOST", "elastic-gbets-master")
#     ES_PORT = os.environ.get("ES_PORT", 9200)
#     kwargs = {
#         "host": ES_HOST,
#         "port": ES_PORT,
#         "timeout": 60 * 5,
#     }
#     logger.debug(f"Trying to connect to {ES_HOST}:{ES_PORT}")

#     # Production Elasticsearch needs credentials
#     ES_USERNAME = os.environ.get("ES_USERNAME")
#     ES_PASSWORD = os.environ.get("ES_PASSWORD")
#     if ES_USERNAME and ES_PASSWORD:
#         kwargs["http_auth"] = (ES_USERNAME, ES_PASSWORD)
#     else:
#         if ES_USERNAME:
#             logger.debug(
#                 f"Auth error: ES_USERNAME has been set, but ES_PASSWORD has not."
#             )
#         else:
#             logger.debug(
#                 f"Auth error: ES_PASSWORD has been set, but ES_USERNAME has not."
#             )

#     yield Elasticsearch(**kwargs)



# with DAG(
#     'dwdi_example',
#     # These args will get passed on to each operator
#     # You can override them on a per-task basis during operator initialization
#     default_args={
#         'depends_on_past': False,
#         'email': ['victor.holanda@cscs.ch'],
#         'email_on_failure': True,
#         'email_on_retry': True,
#         'retries': 1,
#         'retry_delay': timedelta(minutes=5),
#         # 'queue': 'bash_queue',
#         # 'pool': 'backfill',
#         # 'priority_weight': 10,
#         # 'end_date': datetime(2016, 1, 1),
#         # 'wait_for_downstream': False,
#         # 'sla': timedelta(hours=2),
#         # 'execution_timeout': timedelta(seconds=300),
#         # 'on_failure_callback': some_function,
#         # 'on_success_callback': some_other_function,
#         # 'on_retry_callback': another_function,
#         # 'sla_miss_callback': yet_another_function,
#         # 'trigger_rule': 'all_success'
#     },
#     description='DWDI BluePrint Elements',
#     schedule_interval=timedelta(minutes=1),
#     start_date=datetime(2021, 1, 1),
#     catchup=False,
#     tags=['example'],
# ) as dag:

#     # t1, t2 and t3 are examples of tasks created by instantiating operators
#     t1 = BashOperator(
#         task_id='print_date',
#         bash_command='date',
#     )

#     t2 = BashOperator(
#         task_id='sleep',
#         depends_on_past=False,
#         bash_command='sleep 5',
#         retries=3,
#     )
#     t1.doc_md = dedent(
#         """\
#     #### Task Documentation
#     You can document your task using the attributes `doc_md` (markdown),
#     `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
#     rendered in the UI's Task Instance Details page.
#     ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

#     """
#     )

#     dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
#     dag.doc_md = """
#     This is a documentation placed anywhere
#     """  # otherwise, type it like this
#     templated_command = dedent(
#         """
#     {% for i in range(5) %}
#         echo "{{ ds }}"
#         echo "{{ macros.ds_add(ds, 7)}}"
#     {% endfor %}
#     """
#     )

#     t3 = BashOperator(
#         task_id='templated',
#         depends_on_past=False,
#         bash_command=templated_command,
#     )

#     t1 >> [t2, t3]
