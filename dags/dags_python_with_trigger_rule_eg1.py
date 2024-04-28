# all done 테스트

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException

with DAG(
  dag_id='dags_python_with_trigger_rule_eg1',
  schedule=None,
  start_date=pendulum.datetime(2023, 12, 10, tz='Asia/Seoul'),
  catchup=False
) as dag:
    
    bash_upstream_1 = BashOperator(
        task_id = 'bash_upstream_1',
        bash_command = 'echo upstream1'
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception')
    
    @task(task_id='python_upstream2')
    def python_upstream_2():
        print('정상 처리')

    # all_done 모든 태스크가 수행 되어야(실패도 수행으로 봄, 성공 실패 모두 포함) 실행됨
    # - python_upsteam_1 이 실패하긴 하지만 모든 태스크가 수행되기는 하기 때문에 해당 태스크도 실행되어야 함
    @task(task_id='python_downstream_1', trigger_rule='all_done')
    def python_downstream_1():
        print('정상 처리')

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()