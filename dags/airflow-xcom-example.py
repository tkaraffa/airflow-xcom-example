# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
## XCOM Example
Example demonstrating how to pass values from one Airflow task to another.

###[readme](https://github.com/tkaraffa/airflow-xcom-example/blob/main/README.md)  [repo](https://github.com/tkaraffa/airflow-xcom-example)
"""
from datetime import timedelta
from sqlalchemy import *
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.task.context import get_current_context


# query database
def extract_from_db():
    context = get_current_context()
    ti = context["ti"]
    conn_statement = "postgresql://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(conn_statement)
    conn = engine.connect()
    db_statement = """
    SELECT * FROM airflow_xcom_example.example_table;
    """
    results = conn.execute(db_statement).fetchall()
    json_results = [list(row) for row in results]
    ti.xcom_push(key='results', value=json_results)

# get values from one table, double, and prepare for insertion into another table
def double_values():
    context = get_current_context()
    ti = context["ti"]
    results = ti.xcom_pull(key='results', task_ids='extract_from_db')
    doubled_results = []
    for value in results:
        doubled_row = []
        for number in value[1:]:
            doubled_row.append(number * 2)
        doubled_results.append(tuple(value[:1]) + tuple(doubled_row))
    ti.xcom_push(key='doubled_results', value=doubled_results)

# insert new data
def load_into_db():
    context = get_current_context()
    ti = context["ti"]
    doubled_results = ti.xcom_pull(key='doubled_results', task_ids='double_values')
    conn_statement = "postgresql://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(conn_statement)
    conn = engine.connect()
    for row in doubled_results:
        db_statement = f"""
        INSERT INTO airflow_xcom_example.example_table_doubled(
            doubled_id, column1_doubled, column2_doubled)
        VALUES {tuple(row)};
        """
        conn.execute(db_statement)

default_args = {
    'owner': 'tom.karaffa',
    'depends_on_past': False,
    'email': ['tom.karaffa@deep-labs.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(0),
}

with DAG(
    'xcom-example',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
    ) as dag:

    extract_from_db_task = PythonOperator(
        task_id='extract_from_db',
        python_callable=extract_from_db,
    )

    double_values_task = PythonOperator(
        task_id='double_values',
        python_callable=double_values,
    )

    load_into_db_task = PythonOperator(
        task_id='load_into_db',
        python_callable=load_into_db,
    )

    dag.doc_md = __doc__


    extract_from_db_task.doc_md = """
    ## extract_from_db
    Run query to select all data from example table
    """

    double_values_task.doc_md = """
    ## double_values
    Iterate through rows of pulled data, doubling each value,
    and returning as tuple for use in future insert statements
    """

    load_into_db_task.doc_md = """
    ## load_into_db
    Use the previously obtained IP address to insert data into database.
    """

    extract_from_db_task.set_downstream(double_values_task)
    double_values_task.set_downstream(load_into_db_task)
