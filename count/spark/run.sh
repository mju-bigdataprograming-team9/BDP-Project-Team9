# #!/bin/bash
# export LANG=en_US.UTF-8
# export LC_ALL=en_US.UTF-8

# # Python 3.6 경로 설정 (정확한 경로로 수정하세요)
# export PYSPARK_PYTHON='/bin/python3.6'

# spark-submit \
#   --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
#   --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
#   policy_analysis.py  # 실행할 PySpark 파일

#!/bin/bash
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

# Python 3.6 경로 설정
export PYSPARK_PYTHON='/bin/python3.6'

spark-submit \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
  --conf spark.executorEnv.PYSPARK_PYTHON=/bin/python3.6 \
  pt1.py

