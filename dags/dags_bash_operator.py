import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG에 대한 정의
# 모든 DAG에 필요함
with DAG(
  # DAG 의 이름, airflow 대시보드에 나오는 이름, 파이썬 파일명과 통일
  dag_id="dags_bash_operator",
  # 크론식
  schedule="0 0 * * *",
  # DAG 시작일
  start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
  # catchup 이 True 면 start_date 부터 실행 시점(DAG 이 올라온 시점) 사이 빠진 것들도 다 돌림
  # 차례차례 도는 것이 아니라 한꺼번에 돌림.. DAG이 어떻게 만들어졌는가에 따라 문제가 생길 수 있음
  catchup=False
  # 타임아웃, 60 분 이상 돌면 실패 처리, 설정하지 않아도 됨
  # dagrun_timeout=datetime.timedelta(minutes=60),
  # airflow 대시보드에 달리는 태그, 태그들끼리 모아볼 수 있
  # tags=["example", "example2"],
  # 모든 Task 에 공통적으로 넘겨주는 파라미터
  # params={"example_key": "example_value"}
) as dag:
  
  # 태스크
  bash_t1 = BashOperator(
    task_id = "bash_t1",
    bash_command = "echo whoami"
  )
  
  bash_t2 = BashOperator(
    task_id = "bash_t2",
    bash_command = "echo $HOSTNAME"
  )
  
  # 태스크 실행 순서
  bash_t1 >> bash_t2