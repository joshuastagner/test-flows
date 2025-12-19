from time import sleep

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_kubernetes.experimental import kubernetes

def log_on_cancelled(flow, flow_run, state):
    logger = get_run_logger()
    logger.info("cancelled")
    logger.info("flow: ", flow)
    logger.info("flow_run: ", flow_run)
    logger.info("state: ", state)


@task
def sleep_task(n: int):
    logger = get_run_logger()
    logger.info(f"gonna sleep for: {n}")
    sleep(n)
    logger.info(f"waking up")


@flow(on_cancellation=[log_on_cancelled])
def nested_flow(i: str):
    logger = get_run_logger()
    logger.info(f"starting flow {i}")
    sleep_task(120)
    logger.info(f"finished flow {i}")



@flow(on_cancellation=[log_on_cancelled])
def parent_sleep_awhile():
    logger = get_run_logger()
    logger.info("starting some flows")
    for i in range(5):
        nested_flow(str(i))
    logger.info("Gonna sleep while you break stuff....")
    sleep(300)
    