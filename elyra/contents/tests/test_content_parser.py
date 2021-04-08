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
import pytest

from ..file_parser import FileParser


def parse(filepath):
    operation_filepath = os.path.join(os.path.dirname(__file__), filepath)

    operation = FileParser.get_instance(filepath=operation_filepath)
    return operation.get_resources()


def _get_variable_names(model):
    return list(model['env_list'].keys())


def test_python_notebook():
    expected_variable_names = ['VAR1', 'VAR2', 'VAR3', 'VAR4', 'VAR5', 'VAR6', 'VAR7']
    model = parse("resources/parse_python.ipynb")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_r_notebook():
    expected_variable_names = ['VAR1', 'VAR2', 'VAR3', 'VAR4']
    model = parse("resources/parse_r.ipynb")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_python_script():
    expected_variable_names = ['VAR1', 'VAR2', 'VAR3', 'VAR4', 'VAR5', 'VAR6', 'VAR7']
    model = parse("resources/parse.py")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_r_script():
    expected_variable_names = ['VAR1', 'VAR2', 'VAR3', 'VAR4']
    model = parse("resources/parse.r")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_empty_python_notebook():
    expected_variable_names = []
    model = parse("resources/parse_python_empty.ipynb")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_empty_r_notebook():
    expected_variable_names = []
    model = parse("resources/parse_r_empty.ipynb")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_empty_python_script():
    expected_variable_names = []
    model = parse("resources/parse_empty.py")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_empty_r_script():
    expected_variable_names = []
    model = parse("resources/parse_empty.r")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_file_not_found():
    with pytest.raises(FileNotFoundError) as e:
        parse("resources/none.py")
    assert "No such file or directory" in str(e.value)


def test_file_is_not_directory():
    directory = "fakedir.py"
    dir_path = os.path.join(os.path.dirname(__file__), directory)
    os.mkdir(dir_path)

    with pytest.raises(IsADirectoryError) as e:
        parse(dir_path)
    assert "Is a directory" in str(e.value)

    os.rmdir(dir_path)


def test_no_kernel():
    expected_variable_names = []
    model = parse("resources/parse_no_kernel.ipynb")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_parser_not_set():
    expected_variable_names = []
    model = parse("resources/parse_no_language.ipynb")

    variable_names = _get_variable_names(model)
    assert variable_names == expected_variable_names


def test_unsupported_file_type():
    with pytest.raises(ValueError) as e:
        parse("resources/parse.txt")
    assert "Files with extension" and "are not supported" in str(e.value)
