# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the tools hosted by scylla
#############################################################################

import contextlib
import glob
import json
import nodetool
import os
import pytest
import subprocess
import tempfile
import random
import shutil
import util

# To run the Scylla tools, we need to run Scylla executable itself, so we
# need to find the path of the executable that was used to run Scylla for
# this test. We do this by trying to find a local process which is listening
# to the address and port to which our our CQL connection is connected.
# If such a process exists, we verify that it is Scylla, and return the
# executable's path. If we can't find the Scylla executable we use
# pytest.skip() to skip tests relying on this executable.
@pytest.fixture(scope="module")
def scylla_path(cql):
    pid = util.local_process_id(cql)
    if not pid:
        pytest.skip("Can't find local Scylla process")
    # Now that we know the process id, use /proc to find the executable.
    try:
        path = os.readlink(f'/proc/{pid}/exe')
    except:
        pytest.skip("Can't find local Scylla executable")
    # Confirm that this executable is a real tool-providing Scylla by trying
    # to run it with the "--list-tools" option
    try:
        subprocess.check_output([path, '--list-tools'])
    except:
        pytest.skip("Local server isn't Scylla")
    return path

# A fixture for finding Scylla's data directory. We get it using the CQL
# interface to Scylla's configuration. Note that if the server is remote,
# the directory retrieved this way may be irrelevant, whether or not it
# exists on the local machine... However, if the same test that uses this
# fixture also uses the scylla_path fixture, the test will anyway be skipped
# if the running Scylla is not on the local machine local.
@pytest.fixture(scope="module")
def scylla_data_dir(cql):
    try:
        return json.loads(
            cql.execute(
                "SELECT value FROM system.config WHERE name = 'data_file_directories'"
            )
            .one()
            .value
        )[0]
    except:
        pytest.skip("Can't find Scylla sstable directory")


def simple_no_clustering_table(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v int) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        x = random.randrange(0, 4)
        if x == 0:
            # partition tombstone
            cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk = {pk}")
        else:
            # live row
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, v) VALUES ({pk}, 0)")

        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def simple_clustering_table(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk1 int, pk2 int, ck1 int, ck2 int, v int, s int STATIC, PRIMARY KEY ((pk1, pk2), ck1, ck2)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            x = random.randrange(0, 8)
            if x == 0:
                # ttl
                cql.execute(f"INSERT INTO {keyspace}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk}, {pk}, {ck}, {ck}, 0) USING TTL 6000")
            elif x == 1:
                # row tombstone
                cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk1 = {pk} AND pk2 = {pk} AND ck1 = {ck} AND ck2 = {ck}")
            elif x == 2:
                # cell tombstone
                cql.execute(f"DELETE v FROM {keyspace}.{table} WHERE pk1 = {pk} AND pk2 = {pk} AND ck1 = {ck} AND ck2 = {ck}")
            elif x == 3:
                # range tombstone
                l = ck * 10
                u = ck * 11
                cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk1 = {pk} AND pk2 = {pk} AND ck1 > {l} AND ck1 < {u}")
            else:
                # live row
                cql.execute(f"INSERT INTO {keyspace}.{table} (pk1, pk2, ck1, ck2, v) VALUES ({pk}, {pk}, {ck}, {ck}, 0)")

        if pk == 5:
            cql.execute(f"UPDATE {keyspace}.{table} SET s = 10 WHERE pk1 = {pk} AND pk2 = {pk}")
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def clustering_table_with_collection(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int, ck int, v1 map<int, text>, v2 set<int>, v3 list<int>, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            map_vals = {f"{p}: '{c}'" for p in range(0, pk) for c in range(0, ck)}
            map_str = ", ".join(map_vals)
            set_list_vals = list(range(0, pk))
            set_list_str = ", ".join(map(str, set_list_vals))
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v1, v2, v3) VALUES ({pk}, {ck}, {{{map_str}}}, {{{set_list_str}}}, [{set_list_str}])")
        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def clustering_table_with_udt(cql, keyspace):
    table = util.unique_name()
    create_type_schema = f"CREATE TYPE IF NOT EXISTS {keyspace}.type1 (f1 int, f2 text)"
    create_table_schema = f" CREATE TABLE {keyspace}.{table} (pk int, ck int, v type1, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(create_type_schema)
    cql.execute(create_table_schema)

    for pk in range(0, 10):
        for ck in range(0, 10):
            cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, {{f1: 100, f2: 'asd'}})")
        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, "; ".join((create_type_schema, create_table_schema))


def table_with_counters(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int PRIMARY KEY, v counter) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    for pk in range(0, 10):
        for _ in range(0, 4):
            cql.execute(f"UPDATE {keyspace}.{table} SET v = v + 1 WHERE pk = {pk};")
        if pk == 5:
            nodetool.flush(cql, f"{keyspace}.{table}")

    nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


@contextlib.contextmanager
def scylla_sstable(table_factory, cql, ks, data_dir):
    table, schema = table_factory(cql, ks)

    schema_file = os.path.join(data_dir, "..", "test_tools_schema.cql")
    with open(schema_file, "w") as f:
        f.write(schema)

    sstables = glob.glob(os.path.join(data_dir, ks, f'{table}-*', '*-Data.db'))

    try:
        yield (schema_file, sstables)
    finally:
        cql.execute(f"DROP TABLE {ks}.{table}")
        os.unlink(schema_file)


def one_sstable(sstables):
    assert len(sstables) > 1
    return [sstables[0]]


def all_sstables(sstables):
    assert len(sstables) > 1
    return sstables


@pytest.mark.parametrize("what", ["index", "compression-info", "summary", "statistics", "scylla-metadata"])
@pytest.mark.parametrize("which_sstables", [one_sstable, all_sstables])
def test_scylla_sstable_dump_component(cql, test_keyspace, scylla_path, scylla_data_dir, what, which_sstables):
    with scylla_sstable(simple_clustering_table, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        out = subprocess.check_output([scylla_path, "sstable", f"dump-{what}", "--schema-file", schema_file] + which_sstables(sstables))

    print(out)

    assert out
    assert json.loads(out)


@pytest.mark.parametrize("table_factory", [
        simple_no_clustering_table,
        simple_clustering_table,
        clustering_table_with_collection,
        clustering_table_with_udt,
        table_with_counters,
])
@pytest.mark.parametrize("merge", [True, False])
@pytest.mark.parametrize("output_format", ["text", "json"])
def test_scylla_sstable_dump_data(cql, test_keyspace, scylla_path, scylla_data_dir, table_factory, merge, output_format):
    with scylla_sstable(simple_clustering_table, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        args = [scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--output-format", output_format]
        if merge:
            args.append("--merge")
        out = subprocess.check_output(args + sstables)

    print(out)

    assert out
    if output_format == "json":
        assert json.loads(out)


@pytest.mark.parametrize("table_factory", [
        simple_no_clustering_table,
        simple_clustering_table,
])
def test_scylla_sstable_write(cql, test_keyspace, scylla_path, scylla_data_dir, table_factory):
    with scylla_sstable(table_factory, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        with tempfile.TemporaryDirectory() as tmp_dir:
            dump_common_args = [scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--output-format", "json", "--merge"]
            generation = util.unique_key_int()

            original_out = subprocess.check_output(dump_common_args + sstables)
            original_json = json.loads(original_out)["sstables"]["anonymous"]

            input_file = os.path.join(tmp_dir, 'input.json')

            with open(input_file, 'w') as f:
                json.dump(original_json, f)

            subprocess.check_call([scylla_path, "sstable", "write", "--schema-file", schema_file, "--input-file", input_file, "--output-dir", tmp_dir, "--generation", str(generation), '--logger-log-level', 'scylla-sstable=trace'])

            sstable_file = os.path.join(tmp_dir, f"me-{generation}-big-Data.db")

            actual_out = subprocess.check_output(dump_common_args + [sstable_file])
            actual_json = json.loads(actual_out)["sstables"]["anonymous"]

            assert actual_json == original_json


def script_consume_test_table_factory(cql, keyspace):
    table = util.unique_name()
    schema = f"CREATE TABLE {keyspace}.{table} (pk int, ck int, v int, s int STATIC, PRIMARY KEY (pk, ck)) WITH compaction = {{'class': 'NullCompactionStrategy'}}"

    cql.execute(schema)

    partitions = 4

    for sst in range(0, 2):
        for pk in range(sst * partitions, (sst + 1) * partitions):
            # static row
            cql.execute(f"UPDATE {keyspace}.{table} SET s = 10 WHERE pk = {pk}")
            # range tombstone
            cql.execute(f"DELETE FROM {keyspace}.{table} WHERE pk = {pk} AND ck >= 0 AND ck <= 4")
            # 2 rows
            for ck in range(0, 4):
                cql.execute(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES ({pk}, {ck}, 0)")

        nodetool.flush(cql, f"{keyspace}.{table}")

    return table, schema


def test_scylla_sstable_script_consume_sstable(cql, test_keyspace, scylla_path, scylla_data_dir):
    script_file = os.path.join(scylla_data_dir, "..", "test_scylla_sstable_script_consume_sstable.lua")

    script = """
wr = Scylla.new_json_writer()
i = 0

function arg(args, arg)
    ret = nil
    wr:key(arg)
    if args[arg] then
        ret = tonumber(args[arg])
        wr:int(ret)
    else
        wr:null()
    end
    return ret
end

function basename(path)
    s, e = string.find(string.reverse(path), '/', 1, true)
    return string.sub(path, #path - s + 2)
end

function consume_stream_start(args)
    wr:start_object()
    start_sst = arg(args, "start_sst")
    end_sst = arg(args, "end_sst")
    wr:key("content")
    wr:start_array()
end

function consume_sstable_start(sst)
    wrote_ps = false
    i = i + 1
    if i == start_sst then
        return false
    end
    wr:string(basename(sst.filename))
end

function consume_partition_start(ps)
    if not wrote_ps then
        wr:string("ps")
        wrote_ps = true
    end
end

function consume_sstable_end()
    if i == end_sst then
        return false
    end
end

function consume_stream_end()
    wr:end_array()
    wr:end_object()
end
"""
    with open(script_file, 'w') as f:
        f.write(script)

    with scylla_sstable(script_consume_test_table_factory, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        sst1 = os.path.basename(sstables[0])
        sst2 = os.path.basename(sstables[1])
        def run_scenario(script_args, expected):
            print(f"Scenario: '{script_args}'\n")
            script_args = ["--script-arg", script_args] if script_args else []
            script_args = (
                [
                    scylla_path,
                    "sstable",
                    "script",
                    "--schema-file",
                    schema_file,
                    "--script-file",
                    script_file,
                ]
                + script_args
                + sstables[:2]
            )
            res = json.loads(subprocess.check_output(script_args))
            assert res == expected

        run_scenario("", {'start_sst': None, 'end_sst': None, 'content': [sst1, "ps", sst2, "ps"]})
        run_scenario("start_sst=1", {'start_sst': 1, 'end_sst': None, 'content': [sst2, "ps"]})
        run_scenario("start_sst=2", {'start_sst': 2, 'end_sst': None, 'content': [sst1, "ps"]})
        run_scenario("start_sst=1:end_sst=1", {'start_sst': 1, 'end_sst': 1, 'content': []})
        run_scenario("start_sst=2:end_sst=2", {'start_sst': 2, 'end_sst': 2, 'content': [sst1, "ps"]})
        run_scenario("end_sst=1", {'start_sst': None, 'end_sst': 1, 'content': [sst1, "ps"]})
        run_scenario("end_sst=2", {'start_sst': None, 'end_sst': 2, 'content': [sst1, "ps", sst2, "ps"]})


def test_scylla_sstable_script_slice(cql, test_keyspace, scylla_path, scylla_data_dir):



    class bound:
        @staticmethod
        def unpack_value(value):
            return value if isinstance(value, tuple) else (None, value)

        def __init__(self, value, weight):
            self.token, self.value = self.unpack_value(value)
            self.weight = weight

        def tri_cmp(self, value):
            if self.token is None and self.value is None:
                assert(self.weight)
                return -self.weight
            token, value = self.unpack_value(value)
            res = 0 if token is None else int(token) - int(self.token)
            if res == 0 and value is not None and self.value is not None:
                res = int(value) - int(self.value)
            return res if res else -self.weight

        def get_value(self, lookup_table, is_start):
            if self.token is None and self.value is None:
                return '-inf' if is_start else '+inf'
            if self.value is None:
                return "t{}".format(int(self.token))
            return lookup_table[self.value]

        @staticmethod
        def before(value):
            return bound(value, -1)

        @staticmethod
        def at(value):
            return bound(value, 0)

        @staticmethod
        def after(value):
            return bound(value, 1)



    class interval:
        def __init__(self, start_bound, end_bound):
            self.start = start_bound
            self.end = end_bound

        def contains(self, value):
            return self.start.tri_cmp(value) >= 0 and self.end.tri_cmp(value) <= 0


    def summarize_dump(dump):
        summary = []
        for partition in list(dump["sstables"].items())[0][1]:
            partition_summary = {"pk": partition["key"]["value"], "token": partition["key"]["token"], "frags": []}
            if "static_row" in partition:
                partition_summary["frags"].append(("sr", None))
            for clustering_fragment in partition.get("clustering_elements", []):
                type_str = "cr" if clustering_fragment["type"] == "clustering-row" else "rtc"
                partition_summary["frags"].append((type_str, clustering_fragment["key"]["value"]))
            summary.append(partition_summary)
        return summary

    def filter_summary(summary, partition_ranges, clustering_ranges):
        if not partition_ranges:
            return summary
        filtered_summary = []
        for partition in summary:
            if any(map(lambda x: interval.contains(x, (partition["token"], partition["pk"])), partition_ranges)):
                filtered_summary.append({"pk": partition["pk"], "token": partition["token"], "frags": []})
                for (t, k) in partition["frags"]:
                    if t == "rtc" or k is None or not clustering_ranges or any(map(lambda x: interval.contains(x, k), clustering_ranges)):
                        filtered_summary[-1]["frags"].append((t, k))

        return filtered_summary

    def serialize_ranges(prefix, ranges, lookup_table):
        serialized_ranges = []
        i = 0
        for r in ranges:
            s = r.start.get_value(lookup_table, True)
            e = r.end.get_value(lookup_table, False)
            serialized_ranges.append("{}{}={}{},{}{}".format(
                prefix,
                i,
                "(" if s == '-inf' or r.start.weight > 0 else "[",
                s,
                e,
                ")" if e == '+inf' or r.end.weight < 0 else "]"))
            i = i + 1
        return serialized_ranges

    scripts_path = os.path.realpath(os.path.join(__file__, '../../../tools/scylla-sstable-scripts'))
    script_file = os.path.join(scripts_path, 'slice.lua')

    with scylla_sstable(script_consume_test_table_factory, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        reference_summary = summarize_dump(json.loads(subprocess.check_output([scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--merge"] + sstables)))

        # same order as in dump
        pks = [(p["token"], p["pk"]) for p in reference_summary]
        cks = set()
        for p in reference_summary:
            for t, ck in p["frags"]:
                if ck is not None:
                    cks.add(ck)
        cks = sorted(list(cks))
        serialized_pk_lookup = {pk: subprocess.check_output([scylla_path, "types", "serialize", "--full-compound", "-t", "Int32Type", "--", pk]).strip().decode() for t, pk in pks}
        serialized_ck_lookup = {ck: subprocess.check_output([scylla_path, "types", "serialize", "--prefix-compound", "-t", "Int32Type", "--", ck]).strip().decode() for ck in cks}

        script_common_args = [scylla_path, "sstable", "script", "--schema-file", schema_file, "--merge", "--script-file", script_file]

        def run_scenario(scenario, partition_ranges, clustering_ranges):
            print(f"running scenario {scenario}")
            script_args = serialize_ranges("pr", partition_ranges, serialized_pk_lookup) + serialize_ranges("cr", clustering_ranges, serialized_ck_lookup)
            if script_args:
                script_args = ["--script-arg"] + [":".join(script_args)]
            print(f"script_args={script_args}")
            expected = filter_summary(reference_summary, partition_ranges, clustering_ranges)
            out = subprocess.check_output(script_common_args + script_args + sstables)
            summary = summarize_dump(json.loads(out))
            assert summary == expected

        run_scenario("no args", [], [])
        run_scenario("full range", [interval(bound.before(None), bound.after(None))], [])
        run_scenario("(pks[0], +inf)", [interval(bound.after(pks[0]), bound.after(None))], [])
        run_scenario("(-inf, pks[-3]]", [interval(bound.before(None), bound.after(pks[-3]))], [])
        run_scenario("[pks[2], pks[-2]]", [interval(bound.before(pks[2]), bound.after(pks[-2]))], [])
        run_scenario("[pks[0], pks[1]], [pks[2], pks[3]]", [interval(bound.before(pks[1]), bound.after(pks[2])), interval(bound.before(pks[3]), bound.after(pks[4]))], [])
        run_scenario("[t:pks[2], t:pks[-2]]", [interval(bound.before((pks[2][0], None)), bound.after((pks[-2][0], None)))], [])
        run_scenario("full pk range | [-inf, cks[2]]", [interval(bound.before(None), bound.after(None))], [interval(bound.before(None), bound.after(cks[2]))])
        run_scenario("[pks[0], pks[1]] | (cks[0], cks[1]], (cks[2], +inf)", [interval(bound.before(pks[1]), bound.after(pks[2]))],
                     [interval(bound.after(cks[0]), bound.after(cks[1])), interval(bound.after(cks[2]), bound.after(None))])


@pytest.mark.parametrize("table_factory", [
        simple_no_clustering_table,
        simple_clustering_table,
        clustering_table_with_collection,
        clustering_table_with_udt,
        table_with_counters,
])
def test_scylla_sstable_script(cql, test_keyspace, scylla_path, scylla_data_dir, table_factory):
    scripts_path = os.path.realpath(os.path.join(__file__, '../../../tools/scylla-sstable-scripts'))
    slice_script_path = os.path.join(scripts_path, 'slice.lua')
    dump_script_path = os.path.join(scripts_path, 'dump.lua')
    with scylla_sstable(table_factory, cql, test_keyspace, scylla_data_dir) as (schema_file, sstables):
        dump_common_args = [scylla_path, "sstable", "dump-data", "--schema-file", schema_file, "--output-format", "json"]
        script_common_args = [scylla_path, "sstable", "script", "--schema-file", schema_file]

        # without --merge
        cxx_json = json.loads(subprocess.check_output(dump_common_args + sstables))
        dump_lua_json = json.loads(subprocess.check_output(script_common_args + ["--script-file", dump_script_path] + sstables))
        slice_lua_json = json.loads(subprocess.check_output(script_common_args + ["--script-file", slice_script_path] + sstables))

        assert dump_lua_json == cxx_json
        assert slice_lua_json == cxx_json

        # with --merge
        cxx_json = json.loads(subprocess.check_output(dump_common_args + ["--merge"] + sstables))
        dump_lua_json = json.loads(subprocess.check_output(script_common_args + ["--merge", "--script-file", dump_script_path] + sstables))
        slice_lua_json = json.loads(subprocess.check_output(script_common_args + ["--merge", "--script-file", slice_script_path] + sstables))

        assert dump_lua_json == cxx_json
        assert slice_lua_json == cxx_json


@pytest.fixture(scope="function")
def temp_workdir():
    """ Creates a temporary work directory, for the scope of a single test. """
    with tempfile.TemporaryDirectory() as workdir:
        yield workdir


@pytest.fixture(scope="class")
def system_scylla_local_sstable_prepared(cql, scylla_data_dir):
    """ Prepares the system.scylla_local table for the needs of the schema loading tests.

    Namely:
    * Disable auto-compaction for the system and system-schema keyspaces.
    * Flushes said keyspaces.
    * Locates an sstable belonging to system.scylla_local and returns it.
    """
    with nodetool.no_autocompaction_context(cql, "system", "system_schema"):
        # Need to flush system keyspaces whoose sstables we want to meddle
        # with, to make sure they are actually on disk.
        nodetool.flush_keyspace(cql, "system_schema")
        nodetool.flush_keyspace(cql, "system")
        sstables = glob.glob(os.path.join(scylla_data_dir, "system", "scylla_local-*", "*-Data.db"))
        yield sstables[0]


@pytest.fixture(scope="class")
def system_scylla_local_schema_file():
    """ Prepares a schema.cql with the schema of system.scylla_local. """
    with tempfile.NamedTemporaryFile("w+t") as f:
        f.write("CREATE TABLE system.scylla_local (key text PRIMARY KEY, value text)")
        f.flush()
        yield f.name


@pytest.fixture(scope="class")
def scylla_home_dir(scylla_data_dir):
    """ Create a temporary directory structure to be used as SCYLLA_HOME.

    The top-level directory contains a conf dir, which contains a scylla.yaml,
    which has the workdir configuration item set.
    The top level directory can be used as SCYLLA_HOME, while the conf dir can be
    used as SCYLLA_CONF environment variables respectively, allowing scylla sstable
    tool to locate the work-directory of the node.
    """
    with tempfile.TemporaryDirectory() as scylla_home:
        conf_dir = os.path.join(scylla_home, "conf")
        os.mkdir(conf_dir)

        scylla_yaml_file = os.path.join(conf_dir, "scylla.yaml")
        with open(scylla_yaml_file, "w") as f:
            f.write(f"workdir: {os.path.split(scylla_data_dir)[0]}")

        yield scylla_home


@pytest.fixture(scope="class")
def system_scylla_local_reference_dump(scylla_path, system_scylla_local_sstable_prepared):
    """ Produce a reference json dump of the system.scylla_local sstable. """
    dump_reference = subprocess.check_output([
        scylla_path,
        "sstable",
        "dump-data",
        "--output-format", "json",
        "--logger-log-level", "scylla-sstable=debug",
        "--system-schema",
        "--keyspace", "system",
        "--table", "scylla_local",
        system_scylla_local_sstable_prepared])
    dump_reference = json.loads(dump_reference)["sstables"]
    return list(dump_reference.values())[0]


class TestScyllaSsstableSchemaLoading:
    """ Test class containing all the schema loader tests.

    Helps in providing a natural scope of all the specialized fixtures shared by
    these tests.
    """
    keyspace = "system"
    table = "scylla_local"

    def check(self, scylla_path, extra_args, sstable, dump_reference, cwd=None, env=None):
        dump_common_args = [scylla_path, "sstable", "dump-data", "--output-format", "json", "--logger-log-level", "scylla-sstable=debug"]
        dump = json.loads(subprocess.check_output(dump_common_args + extra_args + [sstable], cwd=cwd, env=env))["sstables"]
        dump = list(dump.values())[0]
        assert dump == dump_reference

    def copy_sstable_to_external_dir(self, system_scylla_local_sstable_prepared, temp_workdir):
        table_data_dir, sstable_filename = os.path.split(system_scylla_local_sstable_prepared)
        sstable_glob = "-".join(sstable_filename.split("-")[:-1]) + "*"
        sstable_components = glob.glob(os.path.join(table_data_dir, sstable_glob))

        for c in sstable_components:
            shutil.copy(c, temp_workdir)

        return glob.glob(os.path.join(temp_workdir, "*-Data.db"))[0]

    def test_table_dir_system_schema(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump):
        self.check(
                scylla_path,
                ["--system-schema", "--keyspace", self.keyspace, "--table", self.table],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_table_dir_schema_file(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, system_scylla_local_schema_file):
        self.check(
                scylla_path,
                ["--schema-file", system_scylla_local_schema_file],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_table_dir_data_dir(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, scylla_data_dir):
        self.check(
                scylla_path,
                ["--scylla-data-dir", scylla_data_dir, "--keyspace", self.keyspace, "--table", self.table],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_table_dir_scylla_yaml(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, scylla_home_dir):
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        self.check(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", self.keyspace, "--table", self.table],
                system_scylla_local_sstable_prepared,
                system_scylla_local_reference_dump)

    def test_external_dir_system_schema(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--system-schema", "--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump)

    def test_external_dir_schema_file(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, system_scylla_local_schema_file):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--schema-file", system_scylla_local_schema_file],
                ext_sstable,
                system_scylla_local_reference_dump)


    def test_external_dir_data_dir(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_data_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--scylla-data-dir", scylla_data_dir, "--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump)

    def test_external_dir_scylla_yaml(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        scylla_yaml_file = os.path.join(scylla_home_dir, "conf", "scylla.yaml")
        self.check(
                scylla_path,
                ["--scylla-yaml-file", scylla_yaml_file, "--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump)

    def test_external_dir_autodetect_schema_file(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, system_scylla_local_schema_file):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        shutil.copy(system_scylla_local_schema_file, os.path.join(temp_workdir, "schema.cql"))
        self.check(
                scylla_path,
                [],
                ext_sstable,
                system_scylla_local_reference_dump,
                cwd=temp_workdir)

    def test_external_dir_autodetect_conf_dir(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump,
                cwd=scylla_home_dir)

    def test_external_dir_autodetect_conf_dir_conf_env(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        conf_dir = os.path.join(scylla_home_dir, "conf")
        self.check(
                scylla_path,
                ["--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump,
                env={"SCYLLA_CONF": conf_dir})

    def test_external_dir_autodetect_conf_dir_home_env(self, scylla_path, system_scylla_local_sstable_prepared, system_scylla_local_reference_dump, temp_workdir, scylla_home_dir):
        ext_sstable = self.copy_sstable_to_external_dir(system_scylla_local_sstable_prepared, temp_workdir)
        self.check(
                scylla_path,
                ["--keyspace", self.keyspace, "--table", self.table],
                ext_sstable,
                system_scylla_local_reference_dump,
                env={"SCYLLA_HOME": scylla_home_dir})
