from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re
from pyspark.sql.types import StringType

# SparkSession 생성
spark = SparkSession.builder.appName("PolicyAnalysisTest").getOrCreate()

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

# 파일 경로 설정
file_path = "hdfs:///user/maria_dev/crawler/merged_data/merged_201911.csv"  # 실제 파일 경로로 변경하세요.

# 데이터 읽기
data = spark.read.option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv(file_path)

# 'content_text' 컬럼에서 첫 번째와 두 번째 기사를 가져옴
data = data.filter(data['content_text'].isNotNull())
articles = data.select('content_text').take(2)  # 첫 번째와 두 번째 기사 가져오기

# 두 번째 기사 선택
article = articles[1]['content_text']  # 두 번째 기사 선택

# 정책 키워드 등장 횟수 계산
counts_pyspark = {}
for policy_id, policy_words in policy_labels.items():
    total_count = 0
    for word in policy_words:
        count = len(re.findall(re.escape(word), article))
        total_count += count
    counts_pyspark[f'정책 {policy_id}'] = total_count

# 결과 출력
print("PySpark에서 두 번째 기사에 대한 정책 키워드 등장 횟수:")
print(counts_pyspark)

# Spark 세션 종료
spark.stop()
