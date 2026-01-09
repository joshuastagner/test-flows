from prefect import flow


flow.from_source(
    source="https://github.com/joshuastagner/test-flows.git",
    entrypoint="main.py:nested_sleep_awhile"
).deploy(
    name="nested_sleep_awhile",
    work_pool_name="my-ecs-pool"
)