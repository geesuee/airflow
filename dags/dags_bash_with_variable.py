import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
  dag_id="dags_bash_with_variable",
  schedule="10 0 * * *",
  start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
  catchup=False
) as dag:
  
  # 주기적 DAG 파싱 시 마다, DB 연결을 야기 -> 불필요한 부하 -> 권고하지 않는 방식
  var_value = Variable.get("sample_key")
  
  bash_var_1 = BashOperator(
    task_id='bash_var_1',
    bash_command=f'echo variable : {var_value}'
  )
  
  # Jinja template 문법을 활용하여 전역변수를 꺼내는 것이 권고하는 방식
  bash_var_2 = BashOperator(
    task_id='bash_var_2',
    bash_command='echo variable : {{var.value.sample_key}}'
  )
  
  bash_var_1 >> bash_var_2