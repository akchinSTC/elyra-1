import json
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.utils import dates
from airflow.operators.python_operator import PythonOperator
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
run_this = PythonOperator(
    task_id='run_this',
    python_callable=construct_subdag,  # calls the local function
    templates_dict={'message': message},
    dag=mydag,
    provide_context=True,
)
