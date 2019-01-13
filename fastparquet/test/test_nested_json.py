# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os

from fastparquet import ParquetFile, json_writer
from fastparquet.test.util import tempdir

TEST_DATA = "test-data"
_ = tempdir


def test_deep1_write(tempdir):
    filename = os.sep.join([tempdir, TEST_DATA])
    data = [
        {"a": [{"b": 1}, {"b": 2}]}
    ]
    json_writer.write(filename, data)

    new_data = ParquetFile(filename).to_pandas()

    assert set(new_data.columns) == {"a.b"}
    assert new_data['a.b'][0] == [1, 2]


def test_deep2_write(tempdir):
    filename = os.sep.join([tempdir, TEST_DATA])
    data = [
        {"a": [{"b": [1, 2]}, {"b": [3, 4]}]}
    ]
    json_writer.write(filename, data)

    new_data = ParquetFile(filename).to_pandas()

    print(new_data)
    assert set(new_data.columns) == {"a.b"}
    assert new_data['a.b'][0] == [[1, 2], [3, 4]]


def test_deep3_write(tempdir):
    filename = os.sep.join([tempdir, TEST_DATA])
    data = [
        {"a": [{"b": {"c": [1, 2]}}, {"b": {"c": [3, 4]}}]}
    ]
    json_writer.write(filename, data)

    new_data = ParquetFile(filename).to_pandas()

    assert set(new_data.columns) == {"a.b.c"}
    # TODO: Is this correct? The data structure says nothing about a.b or a.b.c being null
    assert new_data['a.b.c'][0] == [[1, 2], [3, 4]]


def test_deep4_write(tempdir):
    filename = os.sep.join([tempdir, TEST_DATA])
    data = [
        {"a": [{"b": {"c": [None, 2]}}, {"b": {"c": [3, 4]}}]}
    ]
    json_writer.write(filename, data)

    new_data = ParquetFile(filename).to_pandas()

    assert set(new_data.columns) == {"a.b.c"}
    # TODO: Is this correct? The data structure says nothing about a.b or a.b.c being null
    assert new_data['a.b.c'][0] == [[None, 2], [3, 4]]


def test_simple_write(tempdir):
    filename = os.sep.join([tempdir, TEST_DATA])
    data = [
        {"a": {"b": 1}}
    ]
    json_writer.write(filename, data)

    new_data = ParquetFile(filename).to_pandas()

    assert set(new_data.columns) == {"a.b"}
    assert all(new_data['a.b'] == [1])


def test_write_w_nulls(tempdir):
    filename = os.sep.join([tempdir, TEST_DATA])
    data = [
        {"a": {"b": 1}},
        {"a": {"b": None, "c": 2}},
        {"a": None},
        {"a": {"b": 4}}
    ]
    json_writer.write(filename, data)

    new_data = ParquetFile(filename).to_pandas()

    assert set(new_data.columns) == {"a.b", "a.c"}

    assert new_data['a.b'][0] == 1
    assert new_data['a.b'][1] is None
    assert new_data['a.b'][2] is None
    assert new_data['a.b'][3] == 4

    assert new_data['a.c'][0] is None
    assert new_data['a.c'][1] == 2
    assert new_data['a.c'][2] is None
    assert new_data['a.c'][3] is None
