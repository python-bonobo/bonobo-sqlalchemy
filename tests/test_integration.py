import functools
import json
import warnings
from time import sleep

import docker
import pytest
import requests
import sqlalchemy
from docker.errors import ImageNotFound
from sqlalchemy import Column, Integer, MetaData, String, Table

import bonobo
import bonobo_sqlalchemy
from bonobo.config import use_context

LABEL = "pytest"
IMAGE = "nginx"


def create_docker_container_fixture(name, *, image=None, tag="latest", ports=None):
    def _fixture():
        nonlocal name, image, tag, ports
        image = image or name
        full_image = "{}:{}".format(image, tag)
        ports = ports or ()

        docker_client = docker.APIClient()

        create_container = functools.partial(
            docker_client.create_container,
            image=full_image,
            labels=[LABEL],
            ports=[port for port in ports],
            host_config=docker_client.create_host_config(port_bindings={port: None for port in ports}),
        )

        try:
            container = create_container()
        except ImageNotFound:
            response = docker_client.pull(full_image)
            lines = [line for line in response.splitlines() if line]
            pull_result = json.loads(lines[-1])
            if "error" in pull_result:
                raise RuntimeError("Could not pull {}: {}".format(image, pull_result["error"]))
            container = create_container()

        docker_client.start(container=container["Id"])
        container_info = docker_client.inspect_container(container.get("Id"))

        yield container_info

        docker_client.remove_container(container=container["Id"], force=True)

    _fixture.__name__ = name
    return pytest.yield_fixture(_fixture)


nginx = create_docker_container_fixture("nginx", ports=[80])
postgres = create_docker_container_fixture("postgres", ports=[5432])


def test_nginx(nginx):
    sleep(0.25)
    base_url = "http://127.0.0.1:" + nginx["NetworkSettings"]["Ports"]["80/tcp"][0]["HostPort"]
    resp = requests.get(base_url)
    assert resp.status_code == 200


def create_engine(user, name, port):
    return sqlalchemy.create_engine(
        "postgresql+psycopg2://{user}@localhost:{port}/{name}".format(user=user, name=name, port=port)
    )


def create_root_engine(port):
    return create_engine("postgres", "postgres", port)


def get_graph(**options):
    return bonobo.Graph(
        bonobo_sqlalchemy.Select("SELECT * FROM table", engine="sqlalchemy.pgengine"),
        bonobo_sqlalchemy.InsertOrUpdate(table_name="table_1", engine="sqlalchemy.pgengine"),
    )


def _execute_sql(engine, sql):
    conn = engine.connect()
    try:
        conn.execute("COMMIT")
        conn.execute(sql)
    except Exception as exc:
        warnings.warn(exc)
    finally:
        conn.close()


def wait_for_postgres(port):
    import psycopg2

    for i in range(30):
        try:
            psycopg2.connect(dbname="postgres", user="postgres", password="", host="127.0.0.1", port=port)
            return
        except psycopg2.OperationalError:
            sleep(1)
    raise RuntimeError("postgres not there")


metadata = MetaData()

TABLE_1 = "table_1"
TABLE_2 = "table_2"

table_1 = Table(TABLE_1, metadata, Column("id", Integer, primary_key=True), Column("value", String(255)))

table_2 = Table(TABLE_2, metadata, Column("id", Integer, primary_key=True), Column("value", String(255)))


@use_context
def extract(context):
    context.set_output_fields(["id", "value"])
    for i in range(10):
        yield i, "value for {}".format(i)


def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain()

    return graph


class Bufferize:
    def __init__(self):
        self.buffer = []

    def __call__(self, *args, **kwargs):
        self.buffer.append((args, kwargs))


def test_postgres(postgres):
    # bonobo.settings.QUIET.set(True)

    db_name = "my_db"
    port = postgres["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
    wait_for_postgres(port)
    root_engine = create_root_engine(port)
    _execute_sql(root_engine, "CREATE ROLE my_user WITH LOGIN PASSWORD '';")
    _execute_sql(
        root_engine,
        'CREATE DATABASE {name} WITH OWNER=my_user TEMPLATE=template0 ENCODING="utf-8"'.format(name=db_name),
    )

    engine = create_engine("my_user", db_name, port)
    metadata.create_all(engine)

    services = {"sqlalchemy.engine": engine}

    graph = bonobo.Graph()
    graph.add_chain(extract, bonobo_sqlalchemy.InsertOrUpdate(TABLE_1))
    assert bonobo.run(graph, services=services)

    buf = Bufferize()
    graph = bonobo.Graph()
    graph.add_chain(bonobo_sqlalchemy.Select("SELECT * FROM " + TABLE_1), buf)
    assert bonobo.run(graph, services=services)
    assert buf.buffer == [
        ((0, "value for 0"), {}),
        ((1, "value for 1"), {}),
        ((2, "value for 2"), {}),
        ((3, "value for 3"), {}),
        ((4, "value for 4"), {}),
        ((5, "value for 5"), {}),
        ((6, "value for 6"), {}),
        ((7, "value for 7"), {}),
        ((8, "value for 8"), {}),
        ((9, "value for 9"), {}),
    ]

    graph = bonobo.Graph(
        bonobo_sqlalchemy.Select("SELECT * FROM " + TABLE_1), bonobo_sqlalchemy.InsertOrUpdate(TABLE_2)
    )
    assert bonobo.run(graph, services=services)

    buf = Bufferize()
    graph = bonobo.Graph()
    graph.add_chain(bonobo_sqlalchemy.Select("SELECT * FROM " + TABLE_2), buf)
    assert bonobo.run(graph, services=services)
    assert buf.buffer == [
        ((0, "value for 0"), {}),
        ((1, "value for 1"), {}),
        ((2, "value for 2"), {}),
        ((3, "value for 3"), {}),
        ((4, "value for 4"), {}),
        ((5, "value for 5"), {}),
        ((6, "value for 6"), {}),
        ((7, "value for 7"), {}),
        ((8, "value for 8"), {}),
        ((9, "value for 9"), {}),
    ]
