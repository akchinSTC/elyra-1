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
from notebook_op import NotebookOp


# Function invoked by the python operator
# Used to construct actual pipeline graph
def subdag(parent_dag_name, child_dag_name, start_date, schedule_interval, **kwargs):
    ti = kwargs['ti']
    arguments = ti.xcom_pull(task_ids='run_this')
    args = json.loads(arguments)

    operator_dict = {}

    sub_dag = DAG('%s.%s' % (parent_dag_name, child_dag_name),
                  schedule_interval=schedule_interval,
                  start_date=start_date,
                  )

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

    return sub_dag
