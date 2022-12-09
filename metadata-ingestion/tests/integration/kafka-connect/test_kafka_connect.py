import subprocess
import time

import pytest
import requests
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-10-25 13:00:00"


def is_mysql_up(container_name: str, port: int) -> bool:
    """A cheap way to figure out if mysql is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep '/var/run/mysqld/mysqld.sock' | grep {port}"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


@freeze_time(FROZEN_TIME)
@pytest.mark.integration_batch_1
def test_kafka_connect_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kafka-connect"
    test_resources_dir_kafka = pytestconfig.rootpath / "tests/integration/kafka"

    # Share Compose configurations between files and projects
    # https://docs.docker.com/compose/extends/
    docker_compose_file = [
        str(test_resources_dir_kafka / "docker-compose.yml"),
        str(test_resources_dir / "docker-compose.override.yml"),
    ]
    with docker_compose_runner(docker_compose_file, "kafka-connect") as docker_services:
        wait_for_port(
            docker_services,
            "test_mysql",
            3306,
            timeout=120,
            checker=lambda: is_mysql_up("test_mysql", 3306),
        )
        wait_for_port(docker_services, "test_broker", 59092, timeout=120)
        wait_for_port(docker_services, "test_connect", 58083, timeout=120)
        docker_services.wait_until_responsive(
            timeout=30,
            pause=1,
            check=lambda: requests.get(
                "http://localhost:58083/connectors",
            ).status_code
            == 200,
        )

        # Creating MySQL source with no transformations , only topic prefix
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data="""{
                        "name": "mysql_source1",
                        "config": {
                            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                            "mode": "incrementing",
                            "incrementing.column.name": "id",
                            "topic.prefix": "test-mysql-jdbc-",
                            "tasks.max": "1",
                            "connection.url": "${env:MYSQL_CONNECTION_URL}"
                        }
                    }
                    """,
        )
        assert r.status_code == 201  # Created
        # Creating MySQL source with regex router transformations , only topic prefix
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data="""{
                        "name": "mysql_source2",
                        "config": {
                            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                            "mode": "incrementing",
                            "incrementing.column.name": "id",
                            "tasks.max": "1",
                            "connection.url": "${env:MYSQL_CONNECTION_URL}",
                            "transforms": "TotalReplacement",
                            "transforms.TotalReplacement.type": "org.apache.kafka.connect.transforms.RegexRouter",
                            "transforms.TotalReplacement.regex": ".*(book)",
                            "transforms.TotalReplacement.replacement": "my-new-topic-$1"
                        }
                    }
                    """,
        )
        assert r.status_code == 201  # Created
        # Creating MySQL source with regex router transformations , no topic prefix, table whitelist
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data="""{
                        "name": "mysql_source3",
                        "config": {
                            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                            "mode": "incrementing",
                            "incrementing.column.name": "id",
                            "table.whitelist": "book",
                            "tasks.max": "1",
                            "connection.url": "${env:MYSQL_CONNECTION_URL}",
                            "transforms": "TotalReplacement",
                            "transforms.TotalReplacement.type": "org.apache.kafka.connect.transforms.RegexRouter",
                            "transforms.TotalReplacement.regex": ".*",
                            "transforms.TotalReplacement.replacement": "my-new-topic"
                        }
                    }
                    """,
        )
        assert r.status_code == 201  # Created
        # Creating MySQL source with query , topic prefix
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data="""{
                        "name": "mysql_source4",
                        "config": {
                            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                            "mode": "incrementing",
                            "incrementing.column.name": "id",
                            "query": "select * from member",
                            "topic.prefix": "query-topic",
                            "tasks.max": "1",
                            "connection.url": "${env:MYSQL_CONNECTION_URL}"
                        }
                    }
                    """,
        )
        assert r.status_code == 201  # Created
        # Creating MySQL source with ExtractTopic router transformations - source dataset not added
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data="""{
                    "name": "mysql_source5",
                    "config": {
                        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                        "mode": "incrementing",
                        "incrementing.column.name": "id",
                        "table.whitelist": "book",
                        "topic.prefix": "test-mysql-jdbc2-",
                        "tasks.max": "1",
                        "connection.url": "${env:MYSQL_CONNECTION_URL}",
                        "transforms": "changetopic",
                        "transforms.changetopic.type": "io.confluent.connect.transforms.ExtractTopic$Value",
                        "transforms.changetopic.field": "name"
                    }
                }
                """,
        )
        assert r.status_code == 201  # Created
        # Creating MySQL sink connector - not added
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data="""{
                        "name": "mysql_sink",
                        "config": {
                            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                            "insert.mode": "insert",
                            "auto.create": true,
                            "topics": "my-topic",
                            "tasks.max": "1",
                            "connection.url": "${env:MYSQL_CONNECTION_URL}"
                        }
                    }
                    """,
        )
        assert r.status_code == 201  # Created

        # Creating Debezium MySQL source connector
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data="""{
                        "name": "debezium-mysql-connector",
                        "config": {
                            "name": "debezium-mysql-connector",
                            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                            "database.hostname": "test_mysql",
                            "database.port": "3306",
                            "database.user": "root",
                            "database.password": "rootpwd",
                            "database.server.name": "debezium.topics",
                            "database.history.kafka.bootstrap.servers": "test_broker:9092",
                            "database.history.kafka.topic": "dbhistory.debeziummysql",
                            "include.schema.changes": "false"
                        }
                    }
                    """,
        )
        assert r.status_code == 201  # Created

        # Creating Postgresql source
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data="""{
                    "name": "postgres_source",
                    "config": {
                        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                        "mode": "incrementing",
                        "incrementing.column.name": "id",
                        "table.whitelist": "member",
                        "topic.prefix": "test-postgres-jdbc-",
                        "tasks.max": "1",
                        "connection.url": "${env:POSTGRES_CONNECTION_URL}"
                    }
                }""",
        )
        assert r.status_code == 201  # Created

        # Creating Generic source
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data="""{
                    "name": "generic_source",
                    "config": {
                        "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
                        "kafka.topic": "my-topic",
                        "quickstart": "product",
                        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "value.converter.schemas.enable": "false",
                        "max.interval": 1000,
                        "iterations": 10000000,
                        "tasks.max": "1"
                    }
                }""",
        )
        r.raise_for_status()
        assert r.status_code == 201  # Created

        # Give time for connectors to process the table data
        time.sleep(60)

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "kafka_connect_to_file.yml").resolve()
        run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "kafka_connect_mces.json",
            golden_path=test_resources_dir / "kafka_connect_mces_golden.json",
            ignore_paths=[],
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration_batch_1
def test_kafka_connect_mongosourceconnect_ingest(
    docker_compose_runner, pytestconfig, tmp_path, mock_time
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kafka-connect"
    test_resources_dir_kafka = pytestconfig.rootpath / "tests/integration/kafka"

    # Share Compose configurations between files and projects
    # https://docs.docker.com/compose/extends/
    docker_compose_file = [
        str(test_resources_dir_kafka / "docker-compose.yml"),
        str(test_resources_dir / "docker-compose.override.yml"),
    ]
    with docker_compose_runner(docker_compose_file, "kafka-connect") as docker_services:
        time.sleep(10)
        # Run the setup.sql file to populate the database.
        command = 'docker exec test_mongo mongo admin -u admin -p admin --eval "rs.initiate();"'
        ret = subprocess.run(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        assert ret.returncode == 0
        time.sleep(10)

        wait_for_port(docker_services, "test_broker", 59092, timeout=120)
        wait_for_port(docker_services, "test_connect", 58083, timeout=120)
        docker_services.wait_until_responsive(
            timeout=30,
            pause=1,
            check=lambda: requests.get(
                "http://localhost:58083/connectors",
            ).status_code
            == 200,
        )

        # Creating MongoDB source
        r = requests.post(
            "http://localhost:58083/connectors",
            headers={"Content-Type": "application/json"},
            data=r"""{
                    "name": "source_mongodb_connector",
                    "config": {
                        "tasks.max": "1",
                        "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
                        "connection.uri": "mongodb://admin:admin@test_mongo:27017",
                        "topic.prefix": "mongodb",
                        "database": "test_db",
                        "collection": "purchases",
                        "copy.existing": true,
                        "copy.existing.namespace.regex": "test_db.purchases",
                        "change.stream.full.document": "updateLookup",
                        "topic.creation.enable": "true",
                        "topic.creation.default.replication.factor": "-1",
                        "topic.creation.default.partitions": "-1",
                        "output.json.formatter": "com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson",
                        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                        "value.converter": "org.apache.kafka.connect.storage.StringConverter",
                        "key.converter.schemas.enable": false,
                        "value.converter.schemas.enable": false,
                        "output.format.key": "schema",
                        "output.format.value": "json",
                        "output.schema.infer.value": false,
                        "publish.full.document.only":true
                    }
                }""",
        )
        r.raise_for_status()
        assert r.status_code == 201  # Created

        # Give time for connectors to process the table data
        time.sleep(60)

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "kafka_connect_to_file.yml").resolve()
        run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "kafka_connect_mces.json",
            golden_path=test_resources_dir / "kafka_connect_mongo_mces_golden.json",
            ignore_paths=[],
        )
