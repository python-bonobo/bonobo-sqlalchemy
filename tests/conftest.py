import pprint

import docker
import pytest


"""

LABEL = "pytest"
IMAGE = "nginx"
def pytest_runtest_logreport(report):
    if report.failed:
        docker_client = docker.APIClient()
        test_containers = docker_client.containers(
            all=True,
            filters={"label": LABEL})
        for container in test_containers:
            log_lines = [
                ("docker inspect {!r}:".format(container['Id'])),
                (pprint.pformat(docker_client.inspect_container(container['Id']))),
                ("docker logs {!r}:".format(container['Id'])),
                (docker_client.logs(container['Id'])),
            ]
            report.longrepr.addsection('docker logs', os.linesep.join(log_lines))


def pull_image(image):
    ''' Pull the specified image using docker-py
    This function will parse the result from docker-py and raise an exception
    if there is an error.
    :param image: Name of the image to pull
    '''
    docker_client = docker.APIClient()
    response = docker_client.pull(image)
    lines = [line for line in response.splitlines() if line]

    # The last line of the response contains the overall result of the pull
    # operation.
    pull_result = json.loads(lines[-1])
    if "error" in pull_result:
        raise Exception("Could not pull {}: {}".format(
            image, pull_result["error"]))



@pytest.yield_fixture
def nginx():
    docker_client = docker.APIClient()

    container = docker_client.create_container(
        image=IMAGE,
        labels=[LABEL],
        ports=[80],
        host_config=docker_client.create_host_config(port_bindings={80: None})
    )
    docker_client.start(container=container["Id"])
    container_info = docker_client.inspect_container(container.get('Id'))

    yield container_info

    docker_client.remove_container(container=container["Id"], force=True)
"""
