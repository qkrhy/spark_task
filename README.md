## 프로젝트 개요
Apache Spark를 사용하여 사용자 activity 로그를 Hive table 로 제공하기 위한 Spark Application을 작성
데이터는 CSV 파일 형식으로 제공되며, 각 파일은 한 달치 데이터를 가지고 있음.

## 사전 요구사항
- Python 3.10.9
- Apache Spark 3.x
- Poetry

## 설치
1. git clone <repository-url>
2. 프로젝트 디렉토리로 이동 (cd <project-directory>)
3. Poetry를 사용하여 가상 환경을 만들고 종속성을 설치 : `poetry install`


## 코드 구조
- `initialize_spark()`: Spark 세션을 초기화하고 반환
- `load_processed_periods()`: 이전에 처리된 기간 목록을 로드
- `save_processed_period()`: 처리된 기간을 체크포인트 디렉토리에 저장
- `create_external_table()`: 외부 테이블을 생성
- `process_data()`: 주어진 기간 동안의 데이터를 처리하고 외부 테이블에 삽입
- `main()`: 메인 함수로, Spark 세션을 초기화하고 데이터 처리를 수행

## 사용 방법
1. 패키지를 설치  `pip install pyspark`
2. 데이터 파일(CSV 형식)을 `data` 디렉토리에 배치, 파일 이름은 `YYYY-MMM.csv` 형식 (예: `2019-Jan.csv`, `2019-Feb.csv` 등)
3. `main()` 함수에서 처리할 시작 연도, 월, 종료 연도, 월을 설정
4. 스크립트를 실행 `python main.py`
5. 처리된 데이터는 `table_location`에 지정된 경로에 Parquet 형식으로 저장

## 주의사항
- Python 3.10.9 
- Spark 세션 초기화 시 `spark.driver.bindAddress`와 `spark.driver.port`를 설정
- 처리된 기간은 `check` 디렉토리에 `processed_periods.txt` 파일에 저장
- 데이터 파일이 없는 경우 해당 기간은 건너뜀
- 이전에 처리된 기간은 건너뜀 

# UserActivityProcessin.java 