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

import json

from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_unescape
from tornado import web

from ..util.http import HttpErrorMixin
from .file_parser import FileParser


class FileParserHandler(HttpErrorMixin, APIHandler):
    """Handler to expose REST API to parse envs from a File"""

    @web.authenticated
    async def post(self, path):
        msg_json = dict(title="Operation not supported.")
        self.write(msg_json)
        self.flush()

    @web.authenticated
    async def get(self, path=''):
        path = path or ''
        path = url_unescape(path)

        self.log.debug("Parsing file: %s", path)

        try:
            model = await self.operation_parser(path)
        except FileNotFoundError as fnfe:
            model = dict(title=str(fnfe))
            self.set_status(404)
            raise web.HTTPError(404, str(fnfe)) from fnfe
        except IsADirectoryError as iade:
            model = dict(title=str(iade))
            self.set_status(400)
            raise web.HTTPError(400, str(iade)) from iade
        except Exception:
            model = {"env_list": {}, "inputs": {}, "outputs": {}}
            self.set_status(200)
        else:
            self.set_status(200)
            # TODO: Validation of model

        finally:
            self._finish_request(model)

    async def operation_parser(self, operation_filepath):
        """
        Given the path to a File, will return a dictionary model
        :param operation_filepath: absolute path to a File to be parsed
        :return: a model dict
        """

        operation = FileParser.get_instance(filepath=operation_filepath, parent=self)
        model = operation.get_resources()

        return model

    def _finish_request(self, body):
        """Finish a JSON request with a model."""
        self.set_header('Content-Type', 'application/json')
        self.finish(json.dumps(body))
