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
# from typing import Any, Type, TypeVar, List, Dict
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

        type = self.get_query_argument('type', default=None)
        if type not in {None, 'file', 'notebook'}:
            raise web.HTTPError(400, u'Type %r is invalid' % type)

        self.log.debug("Parsing file: %s", path)

        model = await self.operation_parser(path)

        self.set_status(200)
        # TODO: Validation of model
        self._finish_model(model)

    async def operation_parser(self, operation_filepath):
        """
        Given the path to a File, will return a dictionary model
        :param operation_filepath: absolute path to a File to be parsed
        :return: a model dict
        """

        try:
            operation = FileParser.get_instance(filepath=operation_filepath)
            model = operation.get_resources()
        except Exception as e:
            raise RuntimeError(f'Could not parse file: {e}')

        return model

    def _finish_model(self, model):
        """Finish a JSON request with a model."""
        self.set_header('Content-Type', 'application/json')
        self.finish(json.dumps(model))
