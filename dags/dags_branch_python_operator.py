import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
  dag_id="dags_branch_python_operator",
  start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
  schedule=None,
  catchup=False
) as dag:
  
  # 선행 태스크
  def select_random():
    import random
    
    item_list = ['A', 'B', 'C']
    selected_item = random.choice(item_list)
    
    if selected_item == 'A':
      return 'task_a'
    elif selected_item in ['B', 'C']:
      return ['task_b', 'task_c']
    
    
  python_branch_task = BranchPythonOperator(
    task_id='python_branch_task',
    python_callable=select_random
  )
  
  
  # 후행 태스크 3개(task_a, task_b, task_c)
  def common_func(**kwargs):
    print(kwargs['selected'])
    
  task_a = PythonOperator(
    task_id='task_a',
    python_callable=common_func,
    op_kwargs={'selected' : 'A'}
  )
  
  task_b = PythonOperator(
    task_id='task_b',
    python_callable=common_func,
    op_kwargs={'selected' : 'B'}
  )
  
  task_c = PythonOperator(
    task_id='task_c',
    python_callable=common_func,
    op_kwargs={'selected' : 'C'}
  )
  
  
  python_branch_task >> [task_a, task_b, task_c]