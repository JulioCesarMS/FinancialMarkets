from prefect import serve

from orquestation.prefect_flow import financialmarkets_flow


if __name__ == "__main__":

    financialmarkets_flow.serve(
        name="daily-financialmarkets",
        cron="0 9 * * *"
    )