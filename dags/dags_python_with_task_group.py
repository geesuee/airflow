import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

with DAG(
  dag_id='dags_python_with_task_group',
  schedule=None,
  start_date=pendulum.datetime(2023, 12, 10, tz='Asia/Seoul'),
  catchup=False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)
    
    # 데코레이터로 태스크 그룹화
    @task_group(group_id='first_group')
    def group_1():
        """task_group 데커레이터를 이용한 첫 번쨰 그룹입니다."""
        # 위는 docstring, 해당 함수를 설명하는 주석
        # airflow ui 화면에서 tooltip 으로 확인 가능

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task 입니다.')
        
        inner_function2 = PythonOperator(
            task_id = 'inner_function2',
            python_callable = inner_func,
            op_kwargs = {'msg' : '첫 번째 TaskGroup 내 두 번째 task 입니다.'}
        )

        inner_func1() >> inner_function2
    
    # 클래스로 태스크 그룹화
    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다.') as group_2:
        """여기에 적은 docstring은 표시되지 않습니다. 클래스에서는 tooltip 파라미터에 값을 넣는 것으로 대체합니다."""
        
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print("두 번째 TaskGroup 내 첫 번째 task 입니다.")
        
        inner_function2 = PythonOperator(
            task_id = 'inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg' : '두 번째 TaskGroup 내 두 번째 task 입니다.'}
        )

        inner_func1() >> inner_function2
    

    group_1() >> group_2