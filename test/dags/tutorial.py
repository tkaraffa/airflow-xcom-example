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
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.incubator.apache.org/tutorial.html)
[Tom](https://github.com/tkaraffa)
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
def _get_ip(**kwargs):
    ti = kwargs['ti']
    name = socket.gethostname()
    ip = socket.gethostbyname(name)
    ip = [int(i) for i in ip.split('.')]
    ip[-1] = ip[-1] - 1 # decrement ip address to get to database container's
    ip = '.'.join([str(i) for i in ip])
    ti.xcom_push(key='ip_address', value=ip)


# use the IP address to connect to the database and insert data
def _connect_to_db(**kwargs):
    ti = kwargs['ti']
    ip_address = ti.xcom_pull(key='ip_address', task_ids='getIP')
    conn_statement = f"postgresql://airflow:airflow@{ip_address}:5432/airflow"
    engine = create_engine(conn_statement)
    conn = engine.connect()
    db_statement = """
    INSERT INTO public.example2(column1, column2) VALUES (3333, 333333);
    """
    conn.execute(db_statement)

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'start_date': days_ago(0)
}

with DAG(
    'tutorial',
    default_args=default_args,
    description='getting better',
    schedule_interval=timedelta(days=1)
    ) as dag:

    t1 = PythonOperator(
        task_id='getIP',
        python_callable=_get_ip,
        provide_context=True,
    )

    t1.doc_md = """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    """

    dag.doc_md = __doc__

    t2 = PythonOperator(
        task_id='connectToDB',
        python_callable=_connect_to_db,
        provide_context=True,
    )

    t1.set_downstream(t2)
