# none_skipped 테스트

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException

with DAG(
  dag_id='dags_python_with_trigger_rule_eg2',
  schedule=None,
  start_date=pendulum.datetime(2023, 12, 10, tz='Asia/Seoul'),
  catchup=False
) as dag:
    
    @task.branch(task_id='branching')
    def random_branch():
        import random
        item_list = ['A', 'B', 'C']
        select_item = random.choice(item_list)
        
        if select_item == "A":
            return 'task_a'
        elif select_item == "B":
            return 'task_b'
        elif select_item == "C":
            return 'task_c'
    
    task_a = BashOperator(
        task_id = 'task_a',
        bash_command = 'echo upstream1'
    )

    @task(task_id = 'task_b')
    def task_b():
        print('정상 처리')

    @task(task_id = 'task_c')
    def task_c():
        print('정상 처리')
    
    # 어떤 태스크도 스킵되지 않아야 실행됨
    # - random_branch 에 의해 task_a, task_b, task_c 중 한 개만 실행되고 나머지는 스킵되도록 설계 했기 때문에 해당 태스크는 실행되지 않아야 함
    @task(task_id = 'task_d', trigger_rule='none_skipped')
    def task_d():
        print('정상 처리')

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()