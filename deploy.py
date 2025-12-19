from prefect import flow


flow.from_source(
    source="https://github.com/joshuastagner/test-flows.git",
    entrypoint="nested_flows.py:parent_sleep_awhile"
).deploy(
    name="nested_sleep_awhile",
    work_pool_name="kubernetes-dev-customer-managed"
)