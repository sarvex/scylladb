# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import util
import nodetool
import json

def test_snapshots_table(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, v int') as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (0, 0)")
        nodetool.take_snapshot(cql, table, 'my_tag', False)
        res = list(
            cql.execute(
                "SELECT keyspace_name, table_name, snapshot_name, live, total FROM system.snapshots"
            )
        )
        assert len(res) == 1
        ks, tbl = table.split('.')
        assert res[0][0] == ks
        assert res[0][1] == tbl
        assert res[0][2] == 'my_tag'

def test_clients(scylla_only, cql):
    columns = ', '.join([
        'address',
        'port',
        'client_type',
        'connection_stage',
        'driver_name',
        'driver_version',
        'hostname',
        'protocol_version',
        'shard_id',
        'ssl_cipher_suite',
        'ssl_enabled',
        'ssl_protocol',
        'username',
    ])
    cls = list(cql.execute(f"SELECT {columns} FROM system.clients"))
    for cl in cls:
        assert(cl[0] == '127.0.0.1')
        assert(cl[2] == 'cql')

# We only want to check that the table exists with the listed columns, to assert
# backwards compatibility.
def _check_exists(cql, table_name, columns):
    cols = ", ".join(columns)
    assert list(cql.execute(f"SELECT {cols} FROM system.{table_name}"))

def test_protocol_servers(scylla_only, cql):
    _check_exists(cql, "protocol_servers", ("name", "listen_addresses", "protocol", "protocol_version"))

def test_runtime_info(scylla_only, cql):
    _check_exists(cql, "runtime_info", ("group", "item", "value"))

def test_versions(scylla_only, cql):
    _check_exists(cql, "versions", ("key", "build_id", "build_mode", "version"))

# Check reading the system.config table, which should list all configuration
# parameters. As we noticed in issue #10047, each type of configuration
# parameter can have a different function for printing it out, and some of
# those may be wrong so we want to check as many as we can - including
# specifically the experimental_features option which was wrong in #10047
# and #11003.
def test_system_config_read(scylla_only, cql):
    # All rows should have the columns name, source, type and value:
    rows = list(cql.execute("SELECT name, source, type, value FROM system.config"))
    values = {row.name: row.value for row in rows}
    # Check that experimental_features exists and makes sense.
    # It needs to be a JSON-formatted strings, and the strings need to be
    # ASCII feature names - not binary garbage as it was in #10047,
    # and not numbers-formatted-as-string as in #11003.
    assert 'experimental_features' in values
    obj = json.loads(values['experimental_features'])
    assert isinstance(obj, list)
    assert isinstance(obj[0], str)
    assert obj[0] and obj[0].isascii() and obj[0].isprintable()
    assert not obj[0].isnumeric()  # issue #11003
    # Check formatting of tri_mode_restriction like
    # restrict_replication_simplestrategy. These need to be one of
    # allowed string values 0, 1, true, false or warn - but in particular
    # non-empty and printable ASCII, not garbage.
    assert 'restrict_replication_simplestrategy' in values
    obj = json.loads(values['restrict_replication_simplestrategy'])
    assert isinstance(obj, str)
    assert obj and obj.isascii() and obj.isprintable()
