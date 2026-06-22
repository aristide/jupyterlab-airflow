"""Tests for IR -> Airflow 3.x TaskFlow code generation."""

import ast
import re

import pytest

from jupyterlab_airflow.codegen import generate_dag


def _ir(nodes, edges, **dag):
    base = {
        "dag_id": "my_dag",
        "schedule": "@daily",
        "start_date": "2026-01-01",
        "catchup": False,
        "retries": 1,
        "retry_delay_seconds": 300,
        "tags": ["studio"],
    }
    base.update(dag)
    return {"dag": base, "nodes": nodes, "edges": edges}


def test_taskflow_dag_is_valid_python_and_airflow3():
    ir = _ir(
        nodes=[
            {"id": "n1", "op": "bash", "task_id": "extract",
             "params": {"bash_command": "echo hi"}},
            {"id": "n2", "op": "python_task", "task_id": "transform",
             "params": {"code": "return 1"}},
        ],
        edges=[{"source": "n1", "target": "n2"}],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]

    # Parses and compiles (no execution).
    ast.parse(code)

    # Airflow 3.x imports only — never Airflow-2 paths.
    assert "from airflow.sdk import dag, task" in code
    assert "airflow.operators." not in code
    assert "from airflow.decorators" not in code
    assert "from airflow.models" not in code

    # Provenance header + structure.
    assert code.startswith("# airflow-studio: managed")
    assert "syntax=taskflow" in code
    assert "@task.bash(task_id='extract')" in code
    assert "extract_task >> transform_task" in code
    assert code.rstrip().endswith("my_dag()")


def test_header_carries_afdag_id_for_reassociation():
    ir = _ir(
        nodes=[{"id": "n1", "op": "bash", "task_id": "t",
                "params": {"bash_command": "echo hi"}}],
        edges=[],
    )
    ir["provenance"] = {"afdag_id": "abc-123-uuid"}
    code = generate_dag(ir)["code"]
    # afdag_id rides in the provenance header so a deployed DAG stays linked to
    # its `.afdag` across a dag_id rename (PRD §6.1.8(B) / §8.9).
    assert "afdag_id=abc-123-uuid" in code.splitlines()[0]


def test_operators_render_as_assignments():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "empty", "task_id": "start", "params": {}},
            {"id": "b", "op": "trigger_dagrun", "task_id": "fire",
             "params": {"trigger_dag_id": "other"}},
        ],
        edges=[{"source": "a", "target": "b"}],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    assert "start = EmptyOperator(" in code
    assert "fire = TriggerDagRunOperator(" in code
    assert "from airflow.providers.standard.operators.empty import EmptyOperator" in code
    # `dag, task` from airflow.sdk is imported exactly once (no redundant line).
    assert code.count("from airflow.sdk import task\n") == 0


def test_sensors_render_as_airflow3_operators():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "file_sensor", "task_id": "wait_file",
             "params": {"filepath": "/data/in_{{ ds }}.csv"}},
            {"id": "b", "op": "external_task_sensor", "task_id": "wait_etl",
             "params": {"external_dag_id": "ingest", "external_task_id": "load"}},
            {"id": "c", "op": "datetime_sensor", "task_id": "after_six",
             "params": {"target_time": "{{ ds }}T18:00:00+00:00"}},
            {"id": "d", "op": "timedelta_sensor", "task_id": "hold",
             "params": {"delta_seconds": 3600}},
        ],
        edges=[
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
            {"source": "c", "target": "d"},
        ],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)

    # Operator-style instances, Airflow-3 standard-provider imports only.
    assert "wait_file = FileSensor(" in code
    assert "wait_etl = ExternalTaskSensor(" in code
    assert "after_six = DateTimeSensor(" in code
    assert "hold = TimeDeltaSensor(" in code
    assert "from airflow.providers.standard.sensors.filesystem import FileSensor" in code
    assert (
        "from airflow.providers.standard.sensors.external_task import ExternalTaskSensor"
        in code
    )
    assert "airflow.sensors." not in code and "airflow.operators." not in code
    # TimeDeltaSensor reuses the pinned timedelta import; the seconds are int()-
    # coerced so a stringified value can't emit an importable-but-broken delta.
    assert "delta=timedelta(seconds=int(3600))" in code
    # An optional param left out is simply not emitted.
    assert "fs_conn_id" not in code


def test_timedelta_sensor_coerces_a_stringified_value():
    # If delta_seconds reaches the IR as a string, int(...) keeps the emitted
    # timedelta importable instead of `timedelta(seconds='3600')` (a runtime
    # TypeError that still passes ast.parse).
    ir = _ir(
        nodes=[{"id": "n", "op": "timedelta_sensor", "task_id": "hold",
                "params": {"delta_seconds": "3600"}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    assert "delta=timedelta(seconds=int('3600'))" in res["code"]


def test_optional_sensor_param_is_emitted_when_set():
    ir = _ir(
        nodes=[{"id": "a", "op": "file_sensor", "task_id": "wait_file",
                "params": {"filepath": "/data/x", "fs_conn_id": "my_fs"}}],
        edges=[],
    )
    code = generate_dag(ir)["code"]
    assert "fs_conn_id='my_fs'" in code


def test_shortcircuit_renders_taskflow_decorator():
    ir = _ir(
        nodes=[{"id": "n", "op": "shortcircuit", "task_id": "gate",
                "params": {"code": "return bool(rows)"}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    assert "@task.short_circuit(task_id='gate')" in code
    assert "return bool(rows)" in code


def test_latest_only_renders_as_operator():
    ir = _ir(
        nodes=[{"id": "n", "op": "latest_only", "task_id": "only_latest",
                "params": {}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    assert "only_latest = LatestOnlyOperator(" in code
    assert (
        "from airflow.providers.standard.operators.latest_only import LatestOnlyOperator"
        in code
    )


def test_gated_provider_ops_render_airflow3():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "http", "task_id": "call_api",
             "params": {"http_conn_id": "my_api", "endpoint": "v1/orders",
                        "method": "GET"}},
            {"id": "b", "op": "sql", "task_id": "load",
             "params": {"conn_id": "warehouse", "sql": "INSERT INTO t SELECT 1"}},
            {"id": "c", "op": "sql_sensor", "task_id": "wait_rows",
             "params": {"conn_id": "warehouse", "sql": "SELECT count(*) FROM t"}},
        ],
        edges=[{"source": "c", "target": "a"}, {"source": "a", "target": "b"}],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)

    assert "call_api = HttpOperator(" in code
    assert "endpoint='v1/orders'" in code and "method='GET'" in code
    assert "http_conn_id='my_api'" in code
    assert "load = SQLExecuteQueryOperator(" in code
    assert "wait_rows = SqlSensor(" in code
    assert "conn_id='warehouse'" in code
    # Airflow-3 provider imports, never Airflow-2 paths.
    assert "from airflow.providers.http.operators.http import HttpOperator" in code
    assert (
        "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator"
        in code
    )
    assert "from airflow.providers.common.sql.sensors.sql import SqlSensor" in code
    assert "airflow.operators." not in code and "airflow.sensors." not in code


def test_http_optional_params_only_emitted_when_set():
    # No http_conn_id/data/headers -> those kwargs are omitted (operator defaults).
    ir = _ir(
        nodes=[{"id": "n", "op": "http", "task_id": "ping",
                "params": {"endpoint": "health", "method": "GET"}}],
        edges=[],
    )
    code = generate_dag(ir)["code"]
    assert "endpoint='health'" in code
    assert "http_conn_id" not in code
    assert "data=" not in code and "headers=" not in code
    # Optional params come after the required ones, so an omitted http_conn_id
    # leaves no stray blank line inside the call.
    assert "ping',\n\n" not in code

    # With them set -> emitted.
    ir2 = _ir(
        nodes=[{"id": "n", "op": "http", "task_id": "post",
                "params": {"endpoint": "hook", "method": "POST",
                           "data": "payload", "headers": {"X-Key": "1"}}}],
        edges=[],
    )
    code2 = generate_dag(ir2)["code"]
    assert "data='payload'" in code2
    assert "headers={'X-Key': '1'}" in code2


def test_p2_cloud_k8s_ops_render_airflow3():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "kubernetes_pod", "task_id": "pod",
             "params": {"image": "python:3.12-slim", "arguments": ["print(1)"]}},
            {"id": "b", "op": "s3_key_sensor", "task_id": "wait_s3",
             "params": {"bucket_name": "lake", "bucket_key": "in/x.csv"}},
            {"id": "c", "op": "gcs_object_sensor", "task_id": "wait_gcs",
             "params": {"bucket": "gb", "object": "in/x.csv"}},
            {"id": "d", "op": "bigquery_insert_job", "task_id": "bq",
             "params": {"configuration": {"query": {"query": "SELECT 1"}}}},
        ],
        edges=[{"source": "b", "target": "a"}, {"source": "c", "target": "d"}],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)

    assert "pod = KubernetesPodOperator(" in code and "image='python:3.12-slim'" in code
    assert "wait_s3 = S3KeySensor(" in code and "bucket_key='in/x.csv'" in code
    assert "wait_gcs = GCSObjectExistenceSensor(" in code and "object='in/x.csv'" in code
    assert "bq = BigQueryInsertJobOperator(" in code and "configuration=" in code
    assert (
        "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator"
        in code
    )
    assert "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor" in code
    assert (
        "from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor"
        in code
    )
    assert (
        "from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator"
        in code
    )
    assert "airflow.operators." not in code and "airflow.sensors." not in code


def test_v13_lakehouse_p0_ops_render_airflow3():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "s3_create_object", "task_id": "put",
             "params": {"s3_bucket": "lake", "s3_key": "out/x.csv", "data": "a,b\n1,2"}},
            {"id": "b", "op": "sftp_transfer", "task_id": "sftp",
             "params": {"operation": "get", "local_filepath": "/tmp/x",
                        "remote_filepath": "/r/x"}},
            {"id": "c", "op": "sql_column_check", "task_id": "col",
             "params": {"conn_id": "wh", "table": "sales",
                        "column_mapping": {"amount": {"min": {"greater_than": 0}}}}},
            {"id": "d", "op": "sql_table_check", "task_id": "tbl",
             "params": {"conn_id": "wh", "table": "sales",
                        "checks": {"row_count": {"check_statement": "COUNT(*) > 0"}}}},
            {"id": "e", "op": "spark_submit", "task_id": "spark",
             "params": {"application": "/opt/j.py",
                        "application_args": ["--date", "{{ ds }}"]}},
            {"id": "f", "op": "papermill", "task_id": "nb",
             "params": {"input_nb": "/n/in.ipynb", "output_nb": "/n/out.ipynb"}},
            {"id": "g", "op": "email", "task_id": "mail",
             "params": {"to": "a@b.com", "subject": "S", "html_content": "<p>ok</p>"}},
            {"id": "h", "op": "slack_post", "task_id": "slack",
             "params": {"text": "done"}},
        ],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)
    compile(code, "<gen>", "exec")

    assert "put = S3CreateObjectOperator(" in code and "s3_bucket='lake'" in code
    assert "sftp = SFTPOperator(" in code and "operation='get'" in code
    assert "col = SQLColumnCheckOperator(" in code and "column_mapping=" in code
    assert "tbl = SQLTableCheckOperator(" in code and "checks=" in code
    assert "spark = SparkSubmitOperator(" in code and "application='/opt/j.py'" in code
    assert "nb = PapermillOperator(" in code and "input_nb='/n/in.ipynb'" in code
    assert "mail = EmailOperator(" in code and "subject='S'" in code
    assert "slack = SlackAPIPostOperator(" in code and "text='done'" in code

    for imp in (
        "from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator",
        "from airflow.providers.sftp.operators.sftp import SFTPOperator",
        "from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator",
        "from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator",
        "from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator",
        "from airflow.providers.papermill.operators.papermill import PapermillOperator",
        "from airflow.providers.smtp.operators.smtp import EmailOperator",
        "from airflow.providers.slack.operators.slack import SlackAPIPostOperator",
    ):
        assert imp in code, imp
    # Airflow-3 paths only.
    assert "airflow.operators." not in code
    assert "airflow.decorators" not in code
    # Slack with only `text` set: its optional `channel` kwarg is omitted cleanly.
    assert "channel=" not in code


def test_v13_optional_param_omitted_leaves_no_blank():
    # spark_submit with only the required `application` set: the omitted optionals
    # (application_args / name / conn_id) must not leave a stray blank line inside
    # the constructor call (operator blocks are blank-stripped at render time).
    ir = _ir(
        nodes=[{"id": "n", "op": "spark_submit", "task_id": "job",
                "params": {"application": "/opt/j.py"}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    block = re.search(r"job = SparkSubmitOperator\(.*?\n {4}\)", res["code"], re.S)
    assert block, res["code"]
    assert "\n\n" not in block.group(0)
    assert "application='/opt/j.py'" in res["code"]


def test_v13_lakehouse_p1_ops_render_airflow3():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "s3_copy_object", "task_id": "copy",
             "params": {"source_bucket_key": "s/k", "dest_bucket_key": "d/k"}},
            {"id": "b", "op": "s3_list", "task_id": "ls",
             "params": {"bucket": "lake", "prefix": "in/"}},
            {"id": "c", "op": "s3_delete_objects", "task_id": "wipe",
             "params": {"bucket": "lake", "keys": ["a.csv", "b.csv"]}},
            {"id": "d", "op": "sftp_sensor", "task_id": "wait",
             "params": {"path": "/in/", "file_pattern": "*.csv"}},
            {"id": "e", "op": "sftp_to_s3", "task_id": "land",
             "params": {"sftp_path": "/in/x", "s3_bucket": "lake", "s3_key": "raw/x"}},
            {"id": "f", "op": "spark_sql", "task_id": "run_sql",
             "params": {"sql": "SELECT 1"}},
            {"id": "g", "op": "slack_webhook", "task_id": "notify",
             "params": {"slack_webhook_conn_id": "slack_default", "message": "done"}},
        ],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)
    compile(code, "<gen>", "exec")

    assert "copy = S3CopyObjectOperator(" in code and "dest_bucket_key='d/k'" in code
    assert "ls = S3ListOperator(" in code and "bucket='lake'" in code
    # The `keys` param must emit the user's list — NOT collide with dict.keys
    # (Jinja `params.keys` resolves to the method, so the template uses
    # `params['keys']`). This asserts the real value lands.
    assert "wipe = S3DeleteObjectsOperator(" in code
    assert "keys=['a.csv', 'b.csv']" in code
    assert "built-in method" not in code
    assert "wait = SFTPSensor(" in code and "file_pattern='*.csv'" in code
    assert "land = SFTPToS3Operator(" in code and "sftp_path='/in/x'" in code
    assert "run_sql = SparkSqlOperator(" in code and "sql='SELECT 1'" in code
    assert (
        "notify = SlackWebhookOperator(" in code
        and "slack_webhook_conn_id='slack_default'" in code
    )

    for imp in (
        "from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator",
        "from airflow.providers.amazon.aws.operators.s3 import S3ListOperator",
        "from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator",
        "from airflow.providers.sftp.sensors.sftp import SFTPSensor",
        "from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator",
        "from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator",
        "from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator",
    ):
        assert imp in code, imp
    assert "airflow.operators." not in code
    assert "airflow.decorators" not in code


def test_s3_delete_objects_keys_param_avoids_dict_method_collision():
    # The param named `keys` collides with dict.keys in Jinja: `params['keys']`
    # falls back to the bound METHOD when the key is ABSENT, so a legitimate
    # prefix-only delete used to emit `keys=<built-in method ...>` (invalid
    # Python). The template uses `params.get('keys')`; verify BOTH the keys path
    # and the prefix-only path render valid Python with the right kwarg.
    def code_for(params):
        ir = _ir(
            nodes=[{"id": "n", "op": "s3_delete_objects", "task_id": "wipe",
                    "params": params}],
            edges=[],
        )
        res = generate_dag(ir)
        assert res["valid"], res["errors"]
        assert "built-in method" not in res["code"]
        return res["code"]

    c_keys = code_for({"bucket": "b", "keys": ["a.csv", "b.csv"]})
    assert "keys=['a.csv', 'b.csv']" in c_keys
    assert "prefix=" not in c_keys

    c_prefix = code_for({"bucket": "b", "prefix": "tmp/"})
    assert "prefix='tmp/'" in c_prefix
    assert "keys=" not in c_prefix


def test_v13_lakehouse_p2_ops_render_airflow3():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "ftp_file_transmit", "task_id": "ftp",
             "params": {"local_filepath": "/tmp/x", "remote_filepath": "/r/x",
                        "operation": "get", "create_intermediate_dirs": True}},
            {"id": "b", "op": "ftp_sensor", "task_id": "waitftp",
             "params": {"path": "/out/x.csv"}},
            {"id": "c", "op": "imap_attachment_to_s3", "task_id": "imap",
             "params": {"imap_attachment_name": "s.csv", "s3_bucket": "b",
                        "s3_key": "raw/s.csv"}},
            {"id": "d", "op": "imap_attachment_sensor", "task_id": "waitmail",
             "params": {"attachment_name": "s.csv", "check_regex": True}},
            {"id": "e", "op": "spark_jdbc", "task_id": "jdbc",
             "params": {"jdbc_table": "public.s", "metastore_table": "staging.s"}},
            {"id": "f", "op": "spark_kubernetes", "task_id": "sk8s",
             "params": {"template_spec": {"spec": {"image": "spark:3.5"}}}},
            {"id": "g", "op": "discord_webhook", "task_id": "discord",
             "params": {"http_conn_id": "discord_default", "message": "hi", "tts": True}},
            {"id": "h", "op": "telegram", "task_id": "tg",
             "params": {"text": "hi", "chat_id": "-100"}},
            {"id": "i", "op": "opsgenie_create_alert", "task_id": "page",
             "params": {"message": "down", "tags": ["etl"], "details": {"env": "prod"}}},
        ],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)
    compile(code, "<gen>", "exec")

    assert "ftp = FTPFileTransmitOperator(" in code
    # Boolean kwargs render as Python literals, not quoted strings.
    assert "create_intermediate_dirs=True" in code
    assert "check_regex=True" in code
    assert "tts=True" in code
    assert "waitftp = FTPSensor(" in code
    assert "imap = ImapAttachmentToS3Operator(" in code
    assert "waitmail = ImapAttachmentSensor(" in code
    assert "jdbc = SparkJDBCOperator(" in code and "metastore_table='staging.s'" in code
    assert (
        "sk8s = SparkKubernetesOperator(" in code
        and "template_spec={'spec': {'image': 'spark:3.5'}}" in code
    )
    assert (
        "discord = DiscordWebhookOperator(" in code
        and "http_conn_id='discord_default'" in code
    )
    # The real Telegram kwarg is `text`, not `message`.
    assert "tg = TelegramOperator(" in code and "text='hi'" in code
    assert (
        "page = OpsgenieCreateAlertOperator(" in code
        and "tags=['etl']" in code
        and "details={'env': 'prod'}" in code
    )

    for imp in (
        "from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator",
        "from airflow.providers.ftp.sensors.ftp import FTPSensor",
        "from airflow.providers.amazon.aws.transfers.imap_attachment_to_s3 import ImapAttachmentToS3Operator",
        "from airflow.providers.imap.sensors.imap_attachment import ImapAttachmentSensor",
        "from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator",
        "from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator",
        "from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator",
        "from airflow.providers.telegram.operators.telegram import TelegramOperator",
        "from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator",
    ):
        assert imp in code, imp
    assert "airflow.operators." not in code
    assert "airflow.decorators" not in code


def test_operator_block_drops_blank_for_omitted_middle_optional():
    # KubernetesPodOperator with image + env_vars set but the optionals BETWEEN
    # them (name/namespace/cmds/arguments) omitted must not leave a stray blank
    # line inside the constructor call (operator blocks are blank-stripped at
    # render time, since they hold no user code).
    ir = _ir(
        nodes=[{"id": "n", "op": "kubernetes_pod", "task_id": "pod",
                "params": {"image": "img", "env_vars": {"K": "1"}}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    block = re.search(r"pod = KubernetesPodOperator\(.*?\n {4}\)", code, re.S)
    assert block, code
    assert "\n\n" not in block.group(0)
    assert "image='img'" in code and "env_vars={'K': '1'}" in code


def test_code_node_body_blank_lines_are_preserved():
    # The operator-block blank-strip must NOT touch a code node's user-authored
    # body — blank lines the user wrote (incl. inside a multi-line literal) stay.
    body = "vals = [\n    1,\n\n    2,\n]\nreturn sum(vals)"
    ir = _ir(
        nodes=[{"id": "n", "op": "python_task", "task_id": "t",
                "params": {"code": body}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    # Both the between-statements blank and the in-literal blank survive.
    assert "1,\n\n" in res["code"]


def test_per_node_common_params_emitted():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "bash", "task_id": "x",
             "params": {"bash_command": "echo hi"},
             "common": {"retries": 3, "retry_delay": 120, "depends_on_past": True}},
            {"id": "b", "op": "file_sensor", "task_id": "w",
             "params": {"filepath": "/d"},
             "common": {"mode": "reschedule", "poke_interval": 30, "timeout": 600}},
        ],
        edges=[{"source": "a", "target": "b"}],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)
    # Native @task.bash decorator carries the common kwargs; retry_delay is a
    # timedelta, not a bare int.
    assert (
        "@task.bash(task_id='x', retries=3, retry_delay=timedelta(seconds=120), "
        "depends_on_past=True)" in code
    )
    # The operator-type sensor carries the sensor common params.
    assert "mode='reschedule'" in code
    assert "poke_interval=30" in code and "timeout=600" in code


def test_common_params_restricted_to_declared_and_skip_blank():
    # `empty` declares only retries/retry_delay/depends_on_past, so a stray
    # poke_interval is ignored; a blank value is skipped.
    ir = _ir(
        nodes=[{"id": "n", "op": "empty", "task_id": "e", "params": {},
                "common": {"retries": 2, "poke_interval": 30, "retry_delay": ""}}],
        edges=[],
    )
    code = generate_dag(ir)["code"]
    # Inspect only the node's operator call (the DAG default_args also mention
    # retry_delay, so a bare substring check would be confounded).
    call = re.search(r"e = EmptyOperator\(.*?\n {4}\)", code, re.S).group(0)
    assert "retries=2" in call
    assert "poke_interval" not in call  # not declared by `empty`
    assert "retry_delay" not in call  # blank -> skipped


def test_no_common_is_unchanged():
    # A node without a `common` slot emits exactly as before (no trailing kwargs).
    ir = _ir(
        nodes=[{"id": "n", "op": "bash", "task_id": "t",
                "params": {"bash_command": "echo hi"}}],
        edges=[],
    )
    code = generate_dag(ir)["code"]
    assert "@task.bash(task_id='t')" in code


def test_traditional_syntax_renders_with_dag_and_wiring():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "bash", "task_id": "extract",
             "params": {"bash_command": "echo hi"}},
            {"id": "b", "op": "python_task", "task_id": "transform",
             "params": {"code": "return 1"}},
            {"id": "c", "op": "empty", "task_id": "done", "params": {}},
        ],
        edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "c"}],
    )
    ir["syntax_style"] = "traditional"
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)

    # Traditional structure: `with DAG(...) as dag:`, no @dag/@task decorators.
    assert "from airflow.sdk import DAG" in code
    assert "with DAG(" in code and ") as dag:" in code
    assert "@dag(" not in code and "def my_dag():" not in code
    assert "from airflow.sdk import dag, task" not in code
    assert "@task" not in code and "my_dag()" not in code
    # Every op renders as an operator instance — incl. native bash/python.
    assert "extract = BashOperator(" in code
    assert "transform = PythonOperator(" in code
    assert "done = EmptyOperator(" in code
    # The code node's body rides in a nested callable.
    assert "def _transform(**context):" in code and "return 1" in code
    # `>>` wiring + the header tagged traditional.
    assert "extract >> transform" in code and "transform >> done" in code
    assert "syntax=traditional" in code


def _task_graph(code):
    """Extract ({task_ids}, {(src_task_id, tgt_task_id)}) from generated Python,
    resolving handle names to task_ids so TaskFlow (`x_task`) and Traditional
    (`x`) compare equal. The basis for the toggle-equivalence test (PRD §10 / R7)."""
    tree = ast.parse(code)

    def _task_id_of(call):
        for kw in call.keywords:
            if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                return kw.value.value
        return None

    task_ids = set()
    func_to_task = {}  # @task-decorated def name -> task_id (TaskFlow native)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call):
                    tid = _task_id_of(dec)
                    if tid is not None:
                        func_to_task[node.name] = tid
                        task_ids.add(tid)

    handle_to_task = {}  # variable used in `>>` -> task_id
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Call)
        ):
            name = node.targets[0].id
            tid = _task_id_of(node.value)
            if tid is not None:  # `x = SomeOperator(task_id='x', ...)`
                handle_to_task[name] = tid
                task_ids.add(tid)
            elif (
                isinstance(node.value.func, ast.Name)
                and node.value.func.id in func_to_task
            ):  # `x_task = x()` (TaskFlow instantiation)
                handle_to_task[name] = func_to_task[node.value.func.id]

    edges = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.BinOp)
            and isinstance(node.value.op, ast.RShift)
            and isinstance(node.value.left, ast.Name)
            and isinstance(node.value.right, ast.Name)
        ):
            src = handle_to_task.get(node.value.left.id)
            tgt = handle_to_task.get(node.value.right.id)
            if src and tgt:
                edges.add((src, tgt))
    return task_ids, edges


def test_taskflow_and_traditional_yield_the_same_task_graph():
    # PRD §10 / R7: the two backends must be semantically equivalent. They differ
    # in form, but the same IR must produce the same tasks + dependency edges.
    nodes = [
        {"id": "s", "op": "file_sensor", "task_id": "wait",
         "params": {"filepath": "/d"}, "common": {"poke_interval": 30}},
        {"id": "a", "op": "bash", "task_id": "extract",
         "params": {"bash_command": "echo hi"}, "common": {"retries": 2}},
        {"id": "b", "op": "python_task", "task_id": "transform",
         "params": {"code": "return 1"}},
        {"id": "c", "op": "branch", "task_id": "choose",
         "params": {"code": "return 'extract'"}},
        {"id": "d", "op": "empty", "task_id": "done", "params": {}},
    ]
    edges = [
        {"source": "s", "target": "a"},
        {"source": "a", "target": "b"},
        {"source": "c", "target": "a"},
        {"source": "b", "target": "d"},
    ]
    tf = _ir(nodes=nodes, edges=edges)
    tf["syntax_style"] = "taskflow"
    trad = _ir(nodes=nodes, edges=edges)
    trad["syntax_style"] = "traditional"

    tf_code = generate_dag(tf)
    trad_code = generate_dag(trad)
    assert tf_code["valid"] and trad_code["valid"]

    tf_ids, tf_edges = _task_graph(tf_code["code"])
    trad_ids, trad_edges = _task_graph(trad_code["code"])
    assert tf_ids == {"wait", "extract", "transform", "choose", "done"}
    assert tf_ids == trad_ids, (tf_ids, trad_ids)
    assert tf_edges == {
        ("wait", "extract"),
        ("extract", "transform"),
        ("choose", "extract"),
        ("transform", "done"),
    }
    assert tf_edges == trad_edges, (tf_edges, trad_edges)


def test_default_syntax_is_taskflow():
    # No syntax_style on the IR -> TaskFlow (unchanged default).
    ir = _ir(
        nodes=[{"id": "n", "op": "bash", "task_id": "t",
                "params": {"bash_command": "echo hi"}}],
        edges=[],
    )
    code = generate_dag(ir)["code"]
    assert "@dag(" in code and "@task.bash" in code
    assert "with DAG(" not in code
    assert "syntax=taskflow" in code


def test_traditional_emits_common_params_on_instances():
    ir = _ir(
        nodes=[{"id": "n", "op": "file_sensor", "task_id": "w",
                "params": {"filepath": "/d"},
                "common": {"mode": "reschedule", "poke_interval": 30}}],
        edges=[],
    )
    ir["syntax_style"] = "traditional"
    code = generate_dag(ir)["code"]
    assert "w = FileSensor(" in code
    assert "mode='reschedule'" in code and "poke_interval=30" in code


def test_dag_callbacks_render_on_event_callbacks():
    # PRD §6.8: dag['callbacks'] renders on_*_callback kwargs on @dag(...) with
    # each notifier instance built from its registry template, and the notifier
    # imports collected. Multiple notifiers per event + multiple events.
    ir = _ir(
        nodes=[{"id": "n1", "op": "bash", "task_id": "extract",
                "params": {"bash_command": "echo hi"}}],
        edges=[],
        callbacks={
            "on_failure": [
                {"notifier_id": "smtp",
                 "params": {"to": "a@b.com", "subject": "failed"}},
                {"notifier_id": "slack",
                 "params": {"text": "down", "channel": "#alerts"}},
            ],
            "on_success": [{"notifier_id": "slack", "params": {"text": "done"}}],
        },
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)
    assert (
        "on_failure_callback=[SmtpNotifier(to='a@b.com', subject='failed'), "
        "SlackNotifier(text='down', channel='#alerts')]"
    ) in code
    assert "on_success_callback=[SlackNotifier(text='done')]" in code
    assert (
        "from airflow.providers.smtp.notifications.smtp import SmtpNotifier"
        in code
    )
    assert (
        "from airflow.providers.slack.notifications.slack import SlackNotifier"
        in code
    )
    # A notifier param left unset is omitted (the {% if %} guard).
    assert "slack_conn_id=" not in code


def test_dag_callbacks_render_apprise_discord_opsgenie():
    # PRD §6.8 P3 notifiers: each renders its instance into on_*_callback with the
    # right import; opsgenie's `payload` is a dict (emitted as a Python dict).
    ir = _ir(
        nodes=[{"id": "n1", "op": "bash", "task_id": "t",
                "params": {"bash_command": "echo"}}],
        edges=[],
        callbacks={
            "on_failure": [
                {"notifier_id": "apprise",
                 "params": {"body": "down", "tag": "oncall"}},
                {"notifier_id": "discord", "params": {"text": "down"}},
                {"notifier_id": "opsgenie",
                 "params": {"payload": {"message": "down", "priority": "P2"}}},
            ],
        },
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    code = res["code"]
    ast.parse(code)
    assert "AppriseNotifier(body='down', tag='oncall')" in code
    # The Discord NOTIFIER uses discord_conn_id (not http_conn_id like the op).
    assert "DiscordNotifier(text='down')" in code
    # Opsgenie's payload is a dict literal, not a quoted string.
    assert "OpsgenieNotifier(payload={'message': 'down', 'priority': 'P2'})" in code
    for imp in (
        "from airflow.providers.apprise.notifications.apprise import AppriseNotifier",
        "from airflow.providers.discord.notifications.discord import DiscordNotifier",
        "from airflow.providers.opsgenie.notifications.opsgenie import OpsgenieNotifier",
    ):
        assert imp in code, imp


def test_unknown_notifier_is_rejected():
    ir = _ir(
        nodes=[{"id": "n1", "op": "bash", "task_id": "t",
                "params": {"bash_command": "echo"}}],
        edges=[],
        callbacks={"on_failure": [{"notifier_id": "nope", "params": {}}]},
    )
    res = generate_dag(ir)
    assert not res["valid"]
    assert any("notifier" in err.lower() for err in res["errors"])


def test_no_callbacks_emits_no_callback_or_notifier_import():
    ir = _ir(
        nodes=[{"id": "n1", "op": "bash", "task_id": "t",
                "params": {"bash_command": "echo"}}],
        edges=[],
    )
    code = generate_dag(ir)["code"]
    assert "_callback=" not in code
    assert "notifications" not in code


def test_cycle_is_rejected():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "empty", "task_id": "x", "params": {}},
            {"id": "b", "op": "empty", "task_id": "y", "params": {}},
        ],
        edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
    )
    res = generate_dag(ir)
    assert not res["valid"]
    assert any("cycle" in e.lower() for e in res["errors"])


def test_invalid_and_duplicate_identifiers():
    ir = _ir(
        nodes=[
            {"id": "a", "op": "empty", "task_id": "dup", "params": {}},
            {"id": "b", "op": "empty", "task_id": "dup", "params": {}},
        ],
        edges=[],
        dag_id="1bad",
    )
    res = generate_dag(ir)
    assert not res["valid"]
    joined = " ".join(res["errors"])
    assert "dag_id" in joined and "Duplicate" in joined


def test_unknown_operator_is_rejected():
    ir = _ir(
        nodes=[{"id": "a", "op": "does_not_exist", "task_id": "t", "params": {}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert not res["valid"]
    assert "Unknown operator" in res["errors"][0]


def test_string_values_are_escaped_not_injected():
    # A bash_command containing quotes/newlines must round-trip as a safe literal.
    payload = 'echo "hi";\nrm -rf /tmp/x  # not executed'
    ir = _ir(
        nodes=[{"id": "n", "op": "bash", "task_id": "t",
                "params": {"bash_command": payload}}],
        edges=[],
    )
    res = generate_dag(ir)
    assert res["valid"], res["errors"]
    # The payload is emitted via repr() inside the function body; parsing the
    # generated module yields back the exact string with no code injection.
    module = ast.parse(res["code"])
    literals = [
        node.value
        for node in ast.walk(module)
        if isinstance(node, ast.Constant) and node.value == payload
    ]
    assert literals, "bash_command should appear as a single safe string literal"


def test_deterministic_output():
    ir = _ir(
        nodes=[{"id": "n", "op": "bash", "task_id": "t",
                "params": {"bash_command": "echo hi"}}],
        edges=[],
    )
    assert generate_dag(ir)["code"] == generate_dag(ir)["code"]
