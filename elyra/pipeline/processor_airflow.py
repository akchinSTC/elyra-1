#
# Copyright 2018-2021 Elyra Authors
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
from elyra.metadata import MetadataManager
from elyra.pipeline import RuntimePipelineProcess, PipelineProcessorResponse
from elyra.util.path import get_absolute_path
from elyra.util.git import GithubClient
from jinja2 import Environment, PackageLoader


class AirflowPipelineProcessor(RuntimePipelineProcess):
    _type = 'airflow'

    # Provide users with the ability to identify a writable directory in the
    # running container where the notebook | script is executed. The location
    # must exist and be known before the container is started.
    # Defaults to `/tmp`
    WCD = os.getenv('ELYRA_WRITABLE_CONTAINER_DIR', '/tmp').strip().rstrip('/')

    @property
    def type(self):
        return self._type

    def process(self, pipeline):
        t0_all = time.time()
        timestamp = datetime.now().strftime("%m%d%H%M%S")
        pipeline_name = f'{pipeline.name}-{timestamp}'

        runtime_configuration = self._get_metadata_configuration(namespace=MetadataManager.NAMESPACE_RUNTIMES,
                                                                 name=pipeline.runtime_config)
        api_endpoint = runtime_configuration.metadata.get('api_endpoint')
        cos_endpoint = runtime_configuration.metadata.get('cos_endpoint')
        cos_bucket = runtime_configuration.metadata.get('cos_bucket')

        github_api_endpoint = runtime_configuration.metadata.get('github_api_endpoint')
        github_repo_token = runtime_configuration.metadata.get('github_repo_token')
        github_repo = runtime_configuration.metadata.get('github_repo')
        github_branch = runtime_configuration.metadata.get('github_branch')

        self.log_pipeline_info(pipeline_name, "Submitting pipeline")
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline_export_path = os.path.join(temp_dir, f'{pipeline_name}.py')

            self.log.debug("Creating temp directory %s", temp_dir)

            pipeline_filepath = self.create_pipeline_file(pipeline=pipeline,
                                                          pipeline_export_format="py",
                                                          pipeline_export_path=pipeline_export_path,
                                                          pipeline_name=pipeline_name)

            self.log.debug("Uploading pipeline file: %s", pipeline_filepath)

            try:
                github_client = GithubClient(server_url=github_api_endpoint,
                                             token=github_repo_token,
                                             repo=github_repo,
                                             branch=github_branch)

            except BaseException as e:
                raise RuntimeError(f'Unable to create a connection to {github_api_endpoint}: {str(e)}') from e

            github_client.upload_dag(pipeline_filepath, pipeline_name)

            self.log.info('Waiting for Airflow Scheduler to process and start the pipeline')

            github_url = github_client.get_github_url(api_url=github_api_endpoint,
                                                      repository_name=github_repo,
                                                      repository_branch=github_branch)

            self.log_pipeline_info(pipeline_name,
                                   f"pipeline pushed to git: {github_url}",
                                   duration=(time.time() - t0_all))

            return PipelineProcessorResponse(
                # TODO - Add another field to return the url of the job in Airflow UI
                run_url=f'{api_endpoint}',
                object_storage_url=f'{cos_endpoint}',
                object_storage_path=f'/{cos_bucket}/{pipeline_name}',
            )

    def export(self, pipeline, pipeline_export_format, pipeline_export_path, overwrite):
        if pipeline_export_format not in ["py"]:
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

        runtime_configuration = self._get_metadata_configuration(namespace=MetadataManager.NAMESPACE_RUNTIMES,
                                                                 name=pipeline.runtime_config)
        image_namespace = self._get_metadata_configuration(namespace=MetadataManager.NAMESPACE_RUNTIME_IMAGES)

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

            image_pull_policy = None
            for image_instance in image_namespace:
                if image_instance.metadata['image_name'] == operation.runtime_image and \
                        image_instance.metadata.get('pull_policy'):
                    image_pull_policy = image_instance.metadata['pull_policy']

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
                        'image_pull_policy': image_pull_policy,
                        'cpu_request': operation.cpu,
                        'mem_request': operation.memory,
                        'gpu_request': operation.gpu
                        }

            notebook_ops.append(notebook)

            self.log_pipeline_info(pipeline_name,
                                   f"processing operation dependencies for id: {operation.id}",
                                   operation_name=operation.name)

            self._upload_dependencies_to_object_store(runtime_configuration,
                                                      pipeline_name,
                                                      operation)

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
            runtime_configuration = self._get_metadata_configuration(namespace=MetadataManager.NAMESPACE_RUNTIMES,
                                                                     name=pipeline.runtime_config)
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
