from airflow.sdk import dag, task


@dag(schedule=None)
def example_virtualenv():
    @task
    def conn_task():
        import os
        from airflow.hooks.base import BaseHook

        # AIRFLOW_CONN_TEST is defined
        print("AIRFLOW_CONN_TEST", os.environ.get("AIRFLOW_CONN_TEST"))

        # BaseHook.get_connection("test") returns a Connection object
        connection = BaseHook.get_connection("test")
        print("Connection", connection.conn_type)

    @task.virtualenv(
        serializer="dill",
        system_site_packages=True,
    )
    def virtualenv_conn_task():
        import os
        from airflow.hooks.base import BaseHook

        # AIRFLOW_CONN_TEST is defined
        print("AIRFLOW_CONN_TEST", os.environ.get("AIRFLOW_CONN_TEST"))

        # BaseHook.get_connection("test") throws an error, because connection 'test' is not found
        connection = BaseHook.get_connection("test")
        print("Connection", connection.conn_type)

    conn_task()
    virtualenv_conn_task()


example_virtualenv()
