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

from github import Github, GithubException
from traitlets.config import LoggingConfigurable


class GithubClient(LoggingConfigurable):

    def __init__(self,
                 token=None,
                 repo=None,
                 branch=None,
                 ):

        super().__init__()

        self.client = Github(login_or_token=token)
        self.repo = self.client.get_repo(repo)
        self.branch = branch

    def upload_dag(self, pipeline_filepath, pipeline_name):
        try:
            # Upload to github
            with open(pipeline_filepath) as input_file:
                content = input_file.read()

                self.repo.create_file(path=pipeline_name + ".py",
                                      message="Pushed DAG " + pipeline_name,
                                      content=content,
                                      branch=self.branch)

            self.log.info('Pipeline successfully added to the git queue')

        except GithubException as e:
            self.log.debug('Error adding pipeline to git queue: ' + e)
            raise RuntimeError('Error adding pipeline to git queue: ', e)
