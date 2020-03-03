#
# Copyright 2018-2019 IBM Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from dags.sub_dag_template import construct_subdag


# Function invoked by the python operator
# Used to construct actual pipeline graph
def push_to_xcoms(**kwargs):
    string_replace = kwargs.get('templates_dict').get("message").replace("'", '"')
    args = json.loads(string_replace)
    kwargs['ti'].xcom_push(key='arguments', value=string_replace)


# Default arguments to use with Operator
default_args = {'owner': 'elyra',
                'depends_on_past': False,
                'start_date': airflow.utils.dates.days_ago(1),
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'provide_context': True
                }

# DAG Id is the name of the ID to hit with our airflow client in elyra
mydag = DAG(dag_id="elyra",
            default_args=default_args,
            description='Dynamic DAG creation',
            schedule_interval=timedelta(days=1))

message = "{{ dag_run.conf['message']}}"

# Python Operator to kick off the subdag / sub pipeline used to construct the actual graph
run_this = PythonOperator(
    task_id='run_this',
    python_callable=push_to_xcoms,  # calls the local function
    templates_dict={'message': message},
    dag=mydag,
    provide_context=True,
)

run_this_second = SubDagOperator(
    task_id='run_this_second',
    subdag=subdag(mydag, subdag, mydag.start_date,
                  mydag.schedule_interval),
    dag=mydag,
)
