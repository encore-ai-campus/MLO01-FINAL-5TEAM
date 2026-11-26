# 기본 호환성 검사

- 경로 `srcs/support/compatibility/smoke.py`
- 시그니처 Spark 로컬 스크립트
- 파라미터 SparkSession, 임시 Delta 경로
- 호출 loadClass, DataFrameWriter.save, DataFrameReader.load
- 기능 커넥터 로드 및 Delta 3행 쓰기·읽기 확인
