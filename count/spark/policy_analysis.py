import sys
import codecs
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum, length, regexp_replace
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType

# UTF-8 인코딩 설정
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

# SparkSession 생성
spark = SparkSession.builder.appName("PolicyAnalysis").getOrCreate()

# 정책 라벨과 관련 단어 정의
policy_labels = {
    1: ["실거래가", "전매 제한", "경매", "주택 거래", "안정화 대책", "매각",
        "부동산 매매", "매매가", "매물", "매수", "매입", "투기지역",
        "실수요자", "주택구매", "거래절벽"],
    2: ["공공임대", "전월세 상한제", "금융규제", "디딤돌", "보증금", "임대차",
        "전세", "월세", "전세대출", "임차인", "주택담보", "LTV",
        "DTI", "DSR", "계약갱신"],
    3: ["분양가 상한", "주택공급", "택지개발촉진법", "과세", "분상", "재건축",
        "초과이익환수제", "재개", "재시공", "인허가", "조세", "양도세",
        "종합부동산세", "취득세", "부동산세제"]
}

# 입력 데이터 경로 설정 (디렉토리 내의 모든 CSV 파일)
input_path = "hdfs:///user/maria_dev/crawler/merged_data/*.csv"

# 데이터 읽기 (파일명 정보 포함)
data = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path) \
    .withColumn("path", F.input_file_name())

# 파일명에서 월 정보 추출 함수 정의
def extract_month(path):
    match = re.search(r'merged_(\d{6})\.csv', path)
    if match:
        return match.group(1)
    else:
        return None

extract_month_udf = F.udf(extract_month, StringType())

# 월 정보 컬럼 추가
data = data.withColumn('월', extract_month_udf('path'))

# Null 값 제거
data = data.filter(data['content_text'].isNotNull())

# 정책별 키워드 등장 횟수 계산
for policy_id, words in policy_labels.items():
    # 각 키워드별로 등장 횟수 계산 후 합산
    count_exprs = []
    for word in words:
        pattern = re.escape(word)
        word_length = len(word)
        count_expr = (length('content_text') - length(regexp_replace('content_text', pattern, ''))) / word_length
        count_exprs.append(count_expr)
    # 키워드별 등장 횟수 합산
    total_count_expr = sum(count_exprs)
    data = data.withColumn(f'정책 {policy_id}', total_count_expr.cast('int'))

# 정책별 등장 횟수 합산
policy_columns = [f'정책 {policy_id}' for policy_id in policy_labels.keys()]
sum_exprs = [F.sum(col(c)).alias(c) for c in policy_columns]

total_counts = data.groupBy('월').agg(*sum_exprs)

# 중간값 및 상위 10위 값 계산
policy_medians = {}
policy_top10 = {}

for policy_id in policy_labels.keys():
    col_name = f'정책 {policy_id}'
    median = total_counts.approxQuantile(col_name, [0.5], 0)[0]
    top10_values = total_counts.orderBy(col(col_name).desc()).limit(10).select(col_name).collect()
    if len(top10_values) >= 10:
        top10_threshold = top10_values[9][0]
    else:
        top10_threshold = top10_values[-1][0] if top10_values else 0
    policy_medians[col_name] = median
    policy_top10[col_name] = top10_threshold

# 정책type 할당 함수 정의
def assign_policy_type(row):
    valid_policies = {}
    for policy in policy_columns:
        count = row[policy] if row[policy] is not None else 0
        median = policy_medians[policy] if policy_medians[policy] is not None else 0
        top10_threshold = policy_top10[policy] if policy_top10[policy] is not None else 0
        if count > median and count >= top10_threshold:
            valid_policies[policy] = count
    if valid_policies:
        return max(valid_policies, key=valid_policies.get)
    else:
        return "None"

# UDF 등록
assign_policy_type_udf = F.udf(assign_policy_type, StringType())

# 정책type 할당
total_counts = total_counts.withColumn('정책type', assign_policy_type_udf(F.struct([F.col(c) for c in policy_columns])))

# 결과 출력
total_counts.orderBy('월').select('월', *policy_columns, '정책type').show(truncate=False)

# 결과 저장
output_file = "hdfs:///user/maria_dev/output_data/test_result.csv"
total_counts.orderBy('월').select('월', *policy_columns, '정책type') \
    .coalesce(1) \
    .write.mode('overwrite') \
    .option('header', 'true') \
    .csv(output_file)

# Spark 세션 종료
spark.stop()
