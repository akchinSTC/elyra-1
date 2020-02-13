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
from airflow.operators.python_operator import BranchPythonOperator
from notebook_op import NotebookOp


# Function invoked by the python operator
# Used to construct actual pipeline graph
def construct_subdag(**kwargs):
    string_replace = kwargs.get('templates_dict').get("message").replace("'", '"')
    args = json.loads(string_replace)

    operator_dict = {}

    # Default arguments to use with Operator
    default_args = {'owner': 'elyra',
                    'depends_on_past': False,
                    'start_date': dates.days_ago(1),
                    'retries': 0,
                    'retry_delay': timedelta(minutes=5)
                    }
    sub_dag = DAG(dag_id="subdag",
                  default_args=default_args,
                  description='Dynamic DAG creation',
                  schedule_interval=timedelta(days=1))

    # For each dictionary entry create a task with a NotebookOp
    for key, value in args.items():
        print(value)
        with sub_dag:
            elyra_notebook_pod = NotebookOp(task_id=value['id'],
                                            notebook=value['notebook'],
                                            name=value['notebook'],
                                            namespace='default',
                                            image=value['image'],
                                            cos_endpoint=value['cos_endpoint'],
                                            cos_bucket=value['cos_bucket'],
                                            cos_directory=value['cos_directory'],
                                            cos_pull_archive=value['cos_pull_archive'],
                                            pipeline_outputs=value['pipeline_outputs'],
                                            pipeline_inputs=value['pipeline_inputs'],
                                            dependencies=value['dependencies'],
                                            image_pull_policy='IfNotPresent',
                                            get_logs=True,
                                            xcom_push=False
                                            )
        operator_dict[value['id']] = elyra_notebook_pod

    for id_value, node in operator_dict.items():
        if node.dependencies:
            for dependency in node.dependencies:
                node.set_upstream(operator_dict[dependency])

    globals()['test'] = sub_dag

    return operator_dict.keys()


# Default arguments to use with Operator
default_args = {'owner': 'elyra',
                'depends_on_past': False,
                'start_date': airflow.utils.dates.days_ago(1),
                'retries': 0,
                'retry_delay': timedelta(minutes=5)
                }

# DAG Id is the name of the ID to hit with our airflow client in elyra
mydag = DAG(dag_id="elyra",
            default_args=default_args,
            description='Dynamic DAG creation',
            schedule_interval=timedelta(days=1))

message = "{{ dag_run.conf['message']}}"

# Python Operator to kick off the subdag / sub pipeline used to construct the actual graph
run_this = BranchPythonOperator(
    task_id='run_this',
    python_callable=construct_subdag,  # calls the local function
    templates_dict={'message': message},
    op_kwargs={'use_dag': mydag},
    dag=mydag,
    provide_context=True,
)
