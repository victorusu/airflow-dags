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

@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=5),
)

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

def marcoc_dag():

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

    # get_raw_es_data() >> check_es_data_consistency()


# Set the DAG to the one we just implemented
dag = marcoc_dag()

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
