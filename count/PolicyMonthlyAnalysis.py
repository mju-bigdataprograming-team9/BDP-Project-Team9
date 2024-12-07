import re
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum, count
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("PolicyMonthlyAnalysis") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# HDFS 경로
input_directory = "hdfs:///user/maria_dev/crawler/merged_data/"
output_directory = "hdfs:///user/maria_dev/analysis_results/"
output_file = "PolicyMonthlyAnalysis.csv"

# 가중치
title_weight = 2
body_weight = 1

# 정책 키워드 정의
policy_labels = {
    1: ["부동산 매매", "실거래가", "거래량", "매도자", "매수자", "매매가 상승", "시장 안정"],
    2: ["전세", "월세", "임대차", "보증금", "임대료", "임차인", "전월세 상한제", "계약갱신청구권"],
    3: ["LTV", "DTI", "DSR", "대출 한도", "주택담보대출", "금리", "대출 규제", "대출 완화"],
    4: ["종부세", "취득세", "재산세", "양도소득세", "보유세", "세율", "공시가격", "세제 개편"]
}

def process_article(data, year_month):
    # 열 이름에서 불필요한 공백과 제어 문자 제거
    data = data.toDF(*[col_name.strip().replace("\r", "").replace("\n", "").strip() for col_name in data.columns])
    
    # 결측값 제거
    data = data.dropna(subset=["title", "content_text"])
    
    # 제목과 본문을 리스트로 수집
    titles = data.select("title").rdd.flatMap(lambda x: x).collect()
    articles = data.select("content_text").rdd.flatMap(lambda x: x).collect()
    
    counter = Counter()
    
    # 제목과 본문에서 키워드 Count (가중치 적용)
    for title in titles:
        title_words = re.findall(r'\w+', title)
        counter.update({word: count * title_weight for word, count in Counter(title_words).items()})

    for article in articles:
        article_words = re.findall(r'\w+', article)
        counter.update({word: count * body_weight for word, count in Counter(article_words).items()})

    # 정책별 단어 수 계산
    policy_totals = {
        policy_id: sum(counter[word] for word in policy_words) for policy_id, policy_words in policy_labels.items()
    }

    res = [(policy_id, count, year_month) for policy_id, count in policy_totals.items()]

    return spark.createDataFrame(res, ["정책", "단어 수", "연월"])

def process_csv_files(input_directory, output_directory, output_file):
    # HDFS 파일 시스템 객체 생성
    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    input_path = spark._jvm.org.apache.hadoop.fs.Path(input_directory)
    
    # 입력 디렉토리 내의 모든 파일 목록 가져오기
    file_status_list = hadoop_fs.listStatus(input_path)
    file_paths = [file_status.getPath().toString() for file_status in file_status_list]
    
    # 결과를 누적할 DataFrame 초기화
    final_result_df = None
    
    # 각 파일 처리
    for file_path in file_paths:
        print(f"Processing file: {file_path}")
        data = spark.read.option("header", "true").option("multiLine", "true").option("escape", '"').option("quote", '"').option("inferSchema", "true").csv(file_path)
        data = data.toDF(*[col_name.strip().replace("\r", "").replace("\n", "").strip() for col_name in data.columns])
        data = data.select('title', 'content_text')
        
        # 파일명에서 연도와 월 추출
        file_name = file_path.split("/")[-1]
        year_month = re.search(r'(\d{4})(\d{2})', file_name).group(0)
        
        result_df = process_article(data, year_month)
        
        # 결과 누적
        if final_result_df is None:
            final_result_df = result_df
        else:
            final_result_df = final_result_df.union(result_df)
    
    # 각 월별 합산 데이터프레임 생성
    monthly_summary = final_result_df.groupBy("연월", "정책").agg(spark_sum("단어 수").alias("단어 수")).groupBy("연월").pivot("정책").sum("단어 수").fillna(0)

    # 중간값 및 상위 5 값 계산
    policy_medians_dict = {}
    top5_thresholds = {}
    for policy in policy_labels.keys():
        policy_col = str(policy)
        policy_medians_dict[policy] = monthly_summary.approxQuantile(policy_col, [0.5], 0.001)[0]
        top5 = monthly_summary.select(policy_col).orderBy(col(policy_col).desc()).limit(5).collect()
        if len(top5) < 5:
            threshold_val = min([row[policy_col] for row in top5]) if len(top5) > 0 else 0
        else:
            threshold_val = top5[-1][policy_col]
        top5_thresholds[policy] = threshold_val

    # 정책type 할당 함수
    def assign_policy_type(row):
        valid_policies = {}
        for policy, median in policy_medians_dict.items():
            policy_col = str(policy)
            if row[policy_col] > median and row[policy_col] >= top5_thresholds[policy]:
                valid_policies[policy] = row[policy_col]
        if valid_policies:
            selected_policy = max(valid_policies, key=valid_policies.get)
            if selected_policy in [3, 4]:
                return '정책 3'
            return f'정책 {selected_policy}'
        return "None"

    assign_policy_type_udf = F.udf(assign_policy_type, StringType())

    monthly_summary = monthly_summary.withColumn("정책type", assign_policy_type_udf(F.struct([monthly_summary[x] for x in monthly_summary.columns])))

    # 최종 결과 저장
    monthly_summary.repartition(1).write.mode("overwrite").option("header", "true").csv(output_directory)

    # 파일명을 results.csv로 변경
    hdfsUrl = output_directory
    file_path = "part*"
    new_fileName = output_file
    status = hadoop_fs.globStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfsUrl + file_path))
    if status:
        file = status[0].getPath().getName()
        hadoop_fs.rename(spark._jvm.org.apache.hadoop.fs.Path(hdfsUrl + file), spark._jvm.org.apache.hadoop.fs.Path(hdfsUrl + new_fileName))

# main 함수
if __name__ == "__main__":
    process_csv_files(input_directory, output_directory, output_file)

    # 세션 종료
    spark.stop()
