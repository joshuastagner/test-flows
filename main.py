from time import sleep

from prefect import flow, task
from prefect.logging import get_run_logger


@task
def sleep_task(i: str):
    logger = get_run_logger()
    logger.info(f"starting task {i}")
    sleep(300)
    logger.info(f"finished task {i}")


@flow
def sleep_awhile():
    logger = get_run_logger()
    logger.info("starting some tasks")
    for i in range(5):
        sleep_task.submit(i)
    logger.info("Gonna sleep while you break stuff....")
    sleep(120)
    