from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parameterized_flow import parent_flow


docker_block = DockerContainer.load("dtc-docker")

docker_dep = Deployment.build_from_flow(
    flow=parent_flow,
    name='docker_dtc_flow_v2',
    infrastructure=docker_block
)

if __name__=="__main__":
    docker_dep.apply()