import pendulum
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
  dag_id='dags_python_with_macro',
  schedule='10 0 * * *',
  start_date=pendulum.datetime(2023, 12, 10, tz='Asia/Seoul'),
  catchup=False
) as dag:
  
  # start_date : 저번 달 1일
  # end_date : 저번 달 막일
  @task(task_id='task_using_macros', 
        templates_dict={'start_date' : '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, days=1)) | ds }}',
                        'end_date' : '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'})
  def get_datetime_macro(**kwargs):
    templates_dict = kwargs.get('templates_dict') or {}
    if templates_dict:
      start_date = templates_dict.get('start_date') or 'start_date 없음'
      end_date = templates_dict.get('end_date') or 'end_date 없음'
      print(start_date)
      print(end_date)
      
  
  @task(task_id='task_direct_calc')
  def get_datetime_calc(**kwargs):
    from dateutil.relativedelta import relativedelta
    # 스케줄러 부하 경감을 위해
    # 코드 최상단이 아닌, 함수 내부에 import 문 작성
    # 스케줄러는 주기적으로 코드를 파싱
    # 함수 안이 아닌 곳에 import 문을 작성해 놓으면 -> 스케줄러가 부하를 많이 받음
    # 함수 안에 작성한 부분은 주기적으로 파싱(=문법적으로 이상없는지 검사)하지 않음
    # 대규모 환경에서는 스케줄러 부하가 큰 고민
    # 오퍼레이터 안에서만 사용할 라이브러는 task decorater 안에서만 호출 !!
    
    
    data_interval_end = kwargs['date_interval_end']
    prev_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(month=-1, day=1)
    prev_month_day_last = data_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(days=-1)
    print(prev_month_day_first.strftime('%Y-%m-%d'))
    print(prev_month_day_last.strftime('%Y-%m-%d'))
    
  get_datetime_macro() >> get_datetime_calc()