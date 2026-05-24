from prefect import flow
from prefect import serve
from src.pipeline.run_pipeline import run


@flow(name="financialmarkets-flow")
def financialmarkets_flow():
    run()


#
# Ejecutar: python orchestration/deploy.py