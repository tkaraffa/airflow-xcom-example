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

###[readme](https://github.com/tkaraffa/airflow-xcom-example/blob/main/README.md) | ###[repo](https://github.com/tkaraffa/airflow-xcom-example)
"""
from datetime import timedelta
from sqlalchemy import *
import psycopg2
import socket
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.utils.dates import days_ago

# absurd function to get the database container's IP address
def get_ip(**kwargs):
    ti = kwargs['ti']
    name = socket.gethostname()
    ip = socket.gethostbyname(name)
    ip = [int(i) for i in ip.split('.')]
    ip[-1] = ip[-1] - 1 # decrement ip address to get to database container's
    ip = '.'.join([str(i) for i in ip])
    ti.xcom_push(key='ip_address', value=ip)


# use the IP address to connect to the database and insert data
def connect_to_db(**kwargs):
    ti = kwargs['ti']
    ip_address = ti.xcom_pull(key='ip_address', task_ids='get_ip')
    conn_statement = f"postgresql://airflow:airflow@{ip_address}:5432/airflow"
    engine = create_engine(conn_statement)
    conn = engine.connect()
    db_statement = """
    INSERT INTO public.airflow_example(column1, column2) VALUES (3333, 333333);
    """
    conn.execute(db_statement)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

with DAG(
    'xcom-example',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
    ) as dag:

    dag.doc_md = __doc__

    get_ip_task = PythonOperator(
        task_id='get_ip',
        python_callable=get_ip,
        provide_context=True,
    )

    get_ip_task.doc_md = """
    ## get_ip
    Obtain the IP address of the databases container.
     Obtains IP address of current container, and decrements last digit by 1
    <br><br>
    To do: convert to BashOperator, and use command line interface to obtain correct IP address.
    """

    connect_to_db_task = PythonOperator(
        task_id='connect_to_db',
        python_callable=connect_to_db,
        provide_context=True,
    )

    connect_to_db_task.doc_md = """
    ## connect_to_db
    Use the previously obtained IP address to insert data into database.
    """

    get_ip_task.set_downstream(connect_to_db_task)
