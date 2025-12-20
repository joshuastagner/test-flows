from prefect import flow, task, get_run_logger
from prefect.deployments import run_deployment
from prefect.logging.loggers import flow_run_logger
import time
from datetime import datetime, timezone


@task
def trigger(flow_name: str, deployment_name: str, parameters: dict):
    """Triggers a flow deployment."""
    # Prefect converts underscores to hyphens in flow names
    flow_name_normalized = flow_name.replace("_", "-")
    run_deployment(name=f"{flow_name_normalized}/{deployment_name}", parameters=parameters)
    return


@task()
def waiter():
    """Used to wait for previous tasks."""
    pass


def test_nested_sub_flow_running(flow, flow_run, state):
    flow_run_logger(flow_run, flow).info("Running nested sub flow...")
    return


def test_nested_sub_flow_cancellation(flow, flow_run, state):
    flow_run_logger(flow_run, flow).info("Cancelling nested sub flow...")
    return


@flow(
    on_running=[test_nested_sub_flow_running],
    on_cancellation=[test_nested_sub_flow_cancellation],
)
def test_nested_sub_flow(secs: int = 300):
    """Sleeps for some time."""
    logger = get_run_logger()
    logger.info("Starting nested sub flow...")
    time.sleep(secs)
    logger.info("Stopping nested sub flow...")
    return


def test_sub_flow_running(flow, flow_run, state):
    flow_run_logger(flow_run, flow).info("Running sub flow...")
    return


def test_sub_flow_cancellation(flow, flow_run, state):
    flow_run_logger(flow_run, flow).info("Cancelling sub flow...")
    return


@flow(on_running=[test_sub_flow_running], on_cancellation=[test_sub_flow_cancellation])
def test_sub_flow(deployment_name: str, secs: int = 300):
    """
    Triggers the nested sub flow, in both blocking and non-blocking fashions.
    """
    logger = get_run_logger()
    logger.info("Starting sub flow...")

    nonblocking_subflow = trigger.submit(
        test_nested_sub_flow.__name__, deployment_name, {"secs": secs}
    )
    blocking_subflow = trigger(
        test_nested_sub_flow.__name__, deployment_name, {"secs": secs}
    )
    waiter(wait_for=[nonblocking_subflow, blocking_subflow])

    logger.info("Stopping sub flow...")
    return


def test_main_flow_running(flow, flow_run, state):
    flow_run_logger(flow_run, flow).info("Running main flow...")
    return


def test_main_flow_cancellation(flow, flow_run, state):
    flow_run_logger(flow_run, flow).info("Cancelling main flow...")
    return


@flow(
    on_running=[test_main_flow_running], on_cancellation=[test_main_flow_cancellation]
)
def test_main_flow(deployment_name: str, secs: int = 300):
    """
    Triggers the sub flow, in both blocking and non-blocking fashions.
    """
    logger = get_run_logger()
    logger.info("Starting main flow...")

    nonblocking_subflow = trigger.submit(
        test_sub_flow.__name__,
        deployment_name,
        {"deployment_name": deployment_name, "secs": secs},
    )
    blocking_subflow = trigger(
        test_sub_flow.__name__,
        deployment_name,
        {"deployment_name": deployment_name, "secs": secs},
    )
    waiter(wait_for=[nonblocking_subflow, blocking_subflow])

    logger.info("Stopping main flow...")
    return


# test logic
import asyncio

from prefect_release_test.settings import TestSettings
from prefect_release_test.client import TestClient
from prefect_release_test.utils import PermanentError, check_with_retries
# from prefect_release_test.functional_tests.subflow.test import (
#     test_main_flow,
#     test_sub_flow,
#     test_nested_sub_flow,
# )


async def setup(cache: dict):
    """Setup:
    - Deploy the main, sub, and nested sub flows using from_source
    """
    # load
    settings = cache["settings"]
    wait_secs = cache["wait_secs"]
    deployment_name = cache["deployment_name"]

    job_variables = {
        "customizations": '[{"op":"add","path":"/spec/template/spec/containers/0/resources","value":{"requests":{"cpu":"250m","memory":"512Mi"},"limits":{"cpu":"500m","memory":"1Gi"}}}]',
    }

    # Deploy nested sub flow
    nested_sub_flow = await flow.from_source(
        source=settings.github_repo,
        entrypoint="customer_testing_script.py:test_nested_sub_flow",
    )
    nested_sub_flow_deployment_id = await nested_sub_flow.deploy(
        name=deployment_name,
        work_pool_name=settings.workpool_k8s,
        job_variables=job_variables,
        parameters={"secs": wait_secs},
        description="subflow cancellation functional test",
        print_next_steps=False,
    )
    print(f"# Created nested sub flow deployment: '{nested_sub_flow_deployment_id}'")

    # Deploy sub flow
    sub_flow_obj = await flow.from_source(
        source=settings.github_repo,
        entrypoint="customer_testing_script.py:test_sub_flow",
    )
    sub_flow_deployment_id = await sub_flow_obj.deploy(
        name=deployment_name,
        work_pool_name=settings.workpool_k8s,
        job_variables=job_variables,
        parameters={"deployment_name": deployment_name, "secs": wait_secs},
        description="subflow cancellation functional test",
        print_next_steps=False,
    )
    print(f"# Created sub flow deployment: '{sub_flow_deployment_id}'")

    # Deploy main flow
    main_flow_obj = await flow.from_source(
        source=settings.github_repo,
        entrypoint="customer_testing_script.py:test_main_flow",
    )
    main_flow_deployment_id = await main_flow_obj.deploy(
        name=deployment_name,
        work_pool_name=settings.workpool_k8s,
        job_variables=job_variables,
        parameters={"deployment_name": deployment_name, "secs": wait_secs},
        description="subflow cancellation functional test",
        print_next_steps=False,
    )
    print(f"# Created main flow deployment: '{main_flow_deployment_id}'")

    # store
    cache["nested_sub_flow_deployment_id"] = str(nested_sub_flow_deployment_id)
    cache["sub_flow_deployment_id"] = str(sub_flow_deployment_id)
    cache["main_flow_deployment_id"] = str(main_flow_deployment_id)
    return


async def execute(cache: dict):
    """Execute:
    - Trigger a main flowrun, which should trigger two sub flowruns, which in turn
      should trigger four nested sub flowruns
    - Wait till the four nested sub flowruns enter "Running" state
    - Cancel the main flowrun
    """
    # Store test start time to filter out old flowruns
    cache["test_start_time"] = datetime.now(timezone.utc).isoformat()

    # load
    client = cache["client"]
    main_flow_deployment_id = cache["main_flow_deployment_id"]
    nested_sub_flow_deployment_id = cache["nested_sub_flow_deployment_id"]
    expected_nested_sub_flowrun_count = 4

    async with client:
        main_flowrun_id = (await client.run_deployment(main_flow_deployment_id))["id"]
        print("# Triggered main flow deployment")

    async def wait_nested_sub_flowrun():
        async with client:
            count = await client.count_flowruns(
                {
                    "deployments": {"id": {"any_": [nested_sub_flow_deployment_id]}},
                    "flow_runs": {"state": {"name": {"any_": ["Running"]}}},
                }
            )
        assert count == expected_nested_sub_flowrun_count

    # all nested sub flowruns should be running within 7 minutes
    await check_with_retries(wait_nested_sub_flowrun, 14, 30)

    async with client:
        await client.set_flowrun_state(
            main_flowrun_id, {"state": {"type": "CANCELLING", "name": "Cancelling"}}
        )
        print("# Cancelled main flowrun")

    # store
    cache["main_flowrun_id"] = main_flowrun_id
    cache["expected_main_flowrun_count"] = 1
    cache["expected_sub_flowrun_count"] = 2
    cache["expected_nested_sub_flowrun_count"] = 4
    return


async def check(cache: dict):
    """Check:
    - Check that four nested sub flowruns are cancelled, two sub flowruns are
      cancelled, and one main flowruns are cancelled, in that order
    """
    # load
    client = cache["client"]
    nested_sub_flow_deployment_id = cache["nested_sub_flow_deployment_id"]
    sub_flow_deployment_id = cache["sub_flow_deployment_id"]
    main_flow_deployment_id = cache["main_flow_deployment_id"]
    expected_nested_sub_flowrun_count = cache["expected_nested_sub_flowrun_count"]
    expected_sub_flowrun_count = cache["expected_sub_flowrun_count"]
    expected_main_flowrun_count = cache["expected_main_flowrun_count"]
    test_start_time = cache["test_start_time"]

    async def assert_nested_sub_flowrun():
        async with client:
            states = ["Completed", "Failed", "Cancelled", "Crashed"]
            async with asyncio.TaskGroup() as task_group:
                tasks = [
                    task_group.create_task(
                        client.count_flowruns(
                            {
                                "deployments": {
                                    "id": {"any_": [nested_sub_flow_deployment_id]}
                                },
                                "flow_runs": {
                                    "state": {"name": {"any_": [state]}},
                                    "start_time": {"after_": test_start_time},
                                },
                            }
                        )
                    )
                    for state in states
                ]

            actual = {
                states[index]: tasks[index].result() for index in range(len(states))
            }

        if actual["Completed"] > 0 or actual["Failed"] > 0 or actual["Crashed"] > 0:
            raise PermanentError(
                "Detected completed/failed/crashed flowrun, aborting early"
            )
        else:
            assert actual["Cancelled"] == expected_nested_sub_flowrun_count
        print("Assertion succeeded")

    async def assert_sub_flowrun():
        async with client:
            states = ["Completed", "Failed", "Cancelled", "Crashed"]
            async with asyncio.TaskGroup() as task_group:
                tasks = [
                    task_group.create_task(
                        client.count_flowruns(
                            {
                                "deployments": {
                                    "id": {"any_": [sub_flow_deployment_id]}
                                },
                                "flow_runs": {
                                    "state": {"name": {"any_": [state]}},
                                    "start_time": {"after_": test_start_time},
                                },
                            }
                        )
                    )
                    for state in states
                ]

            actual = {
                states[index]: tasks[index].result() for index in range(len(states))
            }

        if actual["Completed"] > 0 or actual["Failed"] > 0 or actual["Crashed"] > 0:
            raise PermanentError(
                "Detected completed/failed/crashed flowrun, aborting early"
            )
        else:
            assert actual["Cancelled"] == expected_sub_flowrun_count
        print("Assertion succeeded")

    async def assert_main_flowrun():
        async with client:
            states = ["Completed", "Failed", "Cancelled", "Crashed"]
            async with asyncio.TaskGroup() as task_group:
                tasks = [
                    task_group.create_task(
                        client.count_flowruns(
                            {
                                "deployments": {
                                    "id": {"any_": [main_flow_deployment_id]}
                                },
                                "flow_runs": {
                                    "state": {"name": {"any_": [state]}},
                                    "start_time": {"after_": test_start_time},
                                },
                            }
                        )
                    )
                    for state in states
                ]

            actual = {
                states[index]: tasks[index].result() for index in range(len(states))
            }

        print(f"DEBUG main_flow counts: {actual}")
        if actual["Completed"] > 0 or actual["Failed"] > 0 or actual["Crashed"] > 0:
            raise PermanentError(
                "Detected completed/failed/crashed flowrun, aborting early"
            )
        else:
            assert actual["Cancelled"] == expected_main_flowrun_count
        print("Assertion succeeded")

    # all nested sub flowruns should be cancelled within 1 minute
    print("# Checking nested sub flowruns")
    await check_with_retries(assert_nested_sub_flowrun, 6, 10)

    # all sub flowruns should be cancelled within 1 minute
    print("# Checking sub flowruns")
    await check_with_retries(assert_sub_flowrun, 6, 10)

    # all main flowruns should be cancelled within 1 minute
    print("# Checking main flowruns")
    await check_with_retries(assert_main_flowrun, 6, 10)

    print("# All assertions succeeded")
    return


async def teardown(cache: dict):
    """Teardown:
    - delete all flowruns
    - delete all deployments
    """
    # load
    client = cache["client"]
    deployment_ids = [
        cache["nested_sub_flow_deployment_id"],
        cache["sub_flow_deployment_id"],
        cache["main_flow_deployment_id"],
    ]

    async with client:
        # delete all flowruns
        async with asyncio.TaskGroup() as task_group:
            tasks = [
                task_group.create_task(
                    client.read_flowruns(
                        {"deployments": {"id": {"any_": [deployment_id]}}}
                    )
                )
                for deployment_id in deployment_ids
            ]

            flowrun_ids = [result["id"] for task in tasks for result in (await task)]
            for flowrun_id in flowrun_ids:
                task_group.create_task(client.delete_flowrun(flowrun_id))
        print("# Deleted all flowruns")

        # delete all deployments
        async with asyncio.TaskGroup() as task_group:
            for deployment_id in deployment_ids:
                task_group.create_task(client.delete_deployment(deployment_id))
        print("# Deleted all deployments")
    return


async def main():
    """Background:
    - We want to make sure that subflows (nested or not) are cancelled when the
      main flow is cancelled
    """
    try:
        print("\n##### FUNCTIONAL TEST: SUBFLOW CANCELLATION")
        settings = TestSettings()
        cache = {
            "settings": settings,
            "client": TestClient(settings),
            "deployment_name": "cancel",
            "wait_secs": 180,
        }

        print("\n### Setup")
        await setup(cache)

        print("\n### Execute")
        await execute(cache)

        print("\n### Check")
        await check(cache)

        # print("\n### Teardown")
        # await teardown(cache)
    except Exception as e:
        raise e
    finally:
        pass
    return


if __name__ == "__main__":
    asyncio.run(main())
