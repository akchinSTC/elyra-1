#
# Copyright 2018-2020 IBM Corporation
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

import os
import autopep8
import json
import re
import tempfile
import time

from black import format_str, FileMode
from collections import OrderedDict
from datetime import datetime
from elyra._version import __version__

from elyra.pipeline import RuntimePipelineProcess, PipelineProcessorResponse

from elyra.util.path import get_absolute_path
from elyra.util.cos import CosClient
from github import Github, GithubException
from jinja2 import Environment, PackageLoader


class AirflowPipelineProcessor(RuntimePipelineProcess):
    _type = 'airflow'

    @property
    def type(self):
        return self._type

    def process(self, pipeline):
        t0_all = time.time()
        timestamp = datetime.now().strftime("%m%d%H%M%S")
        pipeline_name = f'{pipeline.name}-{timestamp}'

        self.log_pipeline_info(pipeline_name, "Submitting pipeline")
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline_export_path = os.path.join(temp_dir, f'{pipeline_name}.py')

            self.log.debug("Creating temp directory %s", temp_dir)

            pipeline_filepath = self.create_pipeline_file(pipeline=pipeline,
                                                          pipeline_export_format="py",
                                                          pipeline_export_path=pipeline_export_path,
                                                          pipeline_name=pipeline_name)

            self.log.debug("Uploading pipeline file: %s", pipeline_filepath)

            runtime_configuration = self._get_runtime_configuration(pipeline.runtime_config)

            cos_endpoint = runtime_configuration.metadata['cos_endpoint']
            cos_bucket = runtime_configuration.metadata['cos_bucket']

            github_repo_token = runtime_configuration.metadata['github_repo_token']
            github_repo = runtime_configuration.metadata['github_repo']
            github_branch = runtime_configuration.metadata['github_branch']

            github_client = Github(login_or_token=github_repo_token)

            try:
                # Upload to github
                repo = github_client.get_repo(github_repo)

                with open(pipeline_filepath) as input_file:
                    content = input_file.read()

                    repo.create_file(path=pipeline_name + ".py",
                                     message="Pushed DAG " + pipeline_name,
                                     content=content,
                                     branch=github_branch)

                self.log.info("Uploaded AirFlow Pipeline...waiting for Airflow Scheduler to start a run")

            except GithubException as e:
                print(e)

            self.log_pipeline_info(pipeline_name,
                                   f"pipeline pushed to git: https://github.com/{github_repo}/tree/{github_branch}",
                                   duration=(time.time() - t0_all))

            # airflow_api_base_url = runtime_configuration.metadata['api_endpoint']

            return PipelineProcessorResponse(
                # TODO - Add another field to return the url of the job in Airflow UI
                run_url=f'https://github.com/{github_repo}/tree/{github_branch}',
                object_storage_url=f'{cos_endpoint}',
                object_storage_path=f'/{cos_bucket}/{pipeline_name}',
            )

    def export(self, pipeline, pipeline_export_format, pipeline_export_path, overwrite):
        if pipeline_export_format not in ["json", "py"]:
            raise ValueError("Pipeline export format {} not recognized.".format(pipeline_export_format))

        timestamp = datetime.now().strftime("%m%d%H%M%S")
        pipeline_name = f'{pipeline.name}-{timestamp}'

        absolute_pipeline_export_path = get_absolute_path(self.root_dir, pipeline_export_path)

        if os.path.exists(absolute_pipeline_export_path) and not overwrite:
            raise ValueError("File " + absolute_pipeline_export_path + " already exists.")

        self.log_pipeline_info(pipeline_name, f"exporting pipeline as a .{pipeline_export_format} file")

        self.create_pipeline_file(pipeline=pipeline,
                                  pipeline_export_format="py",
                                  pipeline_export_path=pipeline_export_path,
                                  pipeline_name=pipeline_name)

        return pipeline_export_path

    def _cc_pipeline(self, pipeline, pipeline_name):

        runtime_configuration = self._get_runtime_configuration(pipeline.runtime_config)

        cos_endpoint = runtime_configuration.metadata['cos_endpoint']
        cos_username = runtime_configuration.metadata['cos_username']
        cos_password = runtime_configuration.metadata['cos_password']
        cos_directory = pipeline_name
        cos_bucket = runtime_configuration.metadata['cos_bucket']

        # Create dictionary that maps component Id to its ContainerOp instance
        notebook_ops = []

        self.log_pipeline_info(pipeline_name,
                               f"processing pipeline dependencies to: {cos_endpoint} "
                               f"bucket: {cos_bucket} folder: {pipeline_name}")

        t0_all = time.time()

        # All previous operation outputs should be propagated throughout the pipeline.
        # In order to process this recursively, the current operation's inputs should be combined
        # from its parent's inputs (which, themselves are derived from the outputs of their parent)
        # and its parent's outputs.
        for operation in pipeline.operations.values():
            parent_io = []  # gathers inputs & outputs relative to parent
            for parent_operation_id in operation.parent_operations:
                parent_operation = pipeline.operations[parent_operation_id]
                if parent_operation.inputs:
                    parent_io.extend(parent_operation.inputs)
                if parent_operation.outputs:
                    parent_io.extend(parent_operation.outputs)

                if parent_io:
                    operation.inputs = parent_io

        for operation in pipeline.operations.values():
            operation_artifact_archive = self._get_dependency_archive_name(operation)

            self.log.debug("Creating pipeline component :\n {op} archive : {archive}".format(
                op=operation, archive=operation_artifact_archive))

            # Collect env variables
            pipeline_envs = dict()
            pipeline_envs['AWS_ACCESS_KEY_ID'] = cos_username
            pipeline_envs['AWS_SECRET_ACCESS_KEY'] = cos_password
            # Convey pipeline logging enablement to operation
            pipeline_envs['ELYRA_ENABLE_PIPELINE_INFO'] = str(self.enable_pipeline_info)

            # Set ENV variables in each container
            if operation.env_vars:
                for env_var in operation.env_vars:
                    # Strip any of these special characters from both key and value
                    # Splits on the first occurrence of '='
                    result = [x.strip(' \'\"') for x in env_var.split('=', 1)]
                    # Should be non empty key with a value
                    if len(result) == 2 and result[0] != '':
                        pipeline_envs[result[0]] = result[1]

            notebook = {'notebook': operation.name,
                        'id': operation.id,
                        'filename': operation.filename,
                        'runtime_image': operation.runtime_image,
                        'cos_endpoint': cos_endpoint,
                        'cos_bucket': cos_bucket,
                        'cos_directory': cos_directory,
                        'cos_dependencies_archive': operation_artifact_archive,
                        'pipeline_outputs': operation.outputs,
                        'pipeline_inputs': operation.inputs,
                        'pipeline_envs': pipeline_envs,
                        'parent_operations': operation.parent_operations,
                        'cpu_request': operation.cpu,
                        'mem_request': operation.memory,
                        'gpu_request': operation.gpu
                        }

            notebook_ops.append(notebook)

            self.log_pipeline_info(pipeline_name,
                                   f"processing operation dependencies for id: {operation.id}",
                                   operation_name=operation.name)

            # upload operation dependencies to object store
            try:
                t0 = time.time()
                dependency_archive_path = self._generate_dependency_archive(operation)
                self.log_pipeline_info(pipeline_name,
                                       f"generated dependency archive: {dependency_archive_path}",
                                       operation_name=operation.name,
                                       duration=(time.time() - t0))

                cos_client = CosClient(config=runtime_configuration)

                t0 = time.time()
                cos_client.upload_file_to_dir(dir=cos_directory,
                                              file_name=operation_artifact_archive,
                                              file_path=dependency_archive_path)
                self.log_pipeline_info(pipeline_name,
                                       f"uploaded dependency archive to: {cos_directory}/{operation_artifact_archive}",
                                       operation_name=operation.name,
                                       duration=(time.time() - t0))

            except FileNotFoundError as ex:
                self.log.error("Dependencies were not found building archive for operation: {}".
                               format(operation.name), exc_info=True)
                raise FileNotFoundError("Node '{}' referenced dependencies that were not found: {}".
                                        format(operation.name, ex))

            except BaseException as ex:
                self.log.error("Error uploading artifacts to object storage for operation: {}".
                               format(operation.name), exc_info=True)
                raise ex from ex

        ordered_notebook_ops = OrderedDict()

        while notebook_ops:
            for i in range(len(notebook_ops)):
                notebook = notebook_ops.pop(0)
                if not notebook['parent_operations']:
                    ordered_notebook_ops[notebook['id']] = notebook
                    self.log.debug("Root Node added : %s", ordered_notebook_ops[notebook['id']])
                elif all(deps in ordered_notebook_ops.keys() for deps in notebook['parent_operations']):
                    ordered_notebook_ops[notebook['id']] = notebook
                    self.log.debug("Dependent Node added : %s", ordered_notebook_ops[notebook['id']])
                else:
                    notebook_ops.append(notebook)

        self.log_pipeline_info(pipeline_name, "pipeline dependencies processed", duration=(time.time() - t0_all))

        return ordered_notebook_ops

    def create_pipeline_file(self, pipeline, pipeline_export_format, pipeline_export_path, pipeline_name):

        self.log.info('Creating pipeline definition as a .' + pipeline_export_format + ' file')
        if pipeline_export_format == "json":
            with open(pipeline_export_path, 'w', encoding='utf-8') as file:
                json.dump(pipeline_export_path, file, ensure_ascii=False, indent=4)
        else:
            # Load template from installed elyra package
            loader = PackageLoader('elyra', 'templates/airflow')
            template_env = Environment(loader=loader)

            template_env.filters['regex_replace'] = lambda string: re.sub("[-!@#$%^&*(){};:,/<>?|`~=+ ]",
                                                                          "_",
                                                                          string)  # nopep8 E731

            template = template_env.get_template('airflow_template.jinja2')

            notebook_ops = self._cc_pipeline(pipeline, pipeline_name)
            runtime_configuration = self._get_runtime_configuration(pipeline.runtime_config)
            user_namespace = runtime_configuration.metadata.get('user_namespace') or 'default'

            description = f"Created with Elyra {__version__} pipeline editor using {pipeline.name}.pipeline."

            python_output = template.render(operations_list=notebook_ops,
                                            pipeline_name=pipeline_name,
                                            namespace=user_namespace,
                                            kube_config_path=None,
                                            is_paused_upon_creation='False',
                                            in_cluster='True',
                                            pipeline_description=description)

            # Write to python file and fix formatting
            with open(pipeline_export_path, "w") as fh:
                autopep_output = autopep8.fix_code(python_output)
                output_to_file = format_str(autopep_output, mode=FileMode())
                fh.write(output_to_file)

        return pipeline_export_path
