from time import sleep

from prefect import flow


@flow
def sleep_awhile():
    print("Gonna sleep while you break stuff....")
    sleep(300)
