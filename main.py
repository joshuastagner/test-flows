from time import sleep

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.docker.docker_image import DockerImage


@task
def sleep_task(i: str):
    logger = get_run_logger()
    logger.info(f"starting task {i}")
    sleep(300)
    logger.info(f"finished task {i}")


@flow
def nested_sleep_awhile():
    logger = get_run_logger()
    logger.info("starting some tasks")
    for i in range(5):
        sleep_task.submit(i)
    logger.info("Gonna sleep while you break stuff....")
    sleep(120)
    

# if __name__ == "__main__":
#     nested_sleep_awhile.deploy(
#         name="bogus-again",
#         work_pool_name="my-ecs-pool",
#         # image="bogus:totally",
#         # image=DockerImage(
#         #     name="bogus",
#         #     tag="totally",
#         #     dockerfile="xDockerfile",
#         #     pull=False
#         # ),
#         push=False,
#         # build kwarg pull -> False
#     )
