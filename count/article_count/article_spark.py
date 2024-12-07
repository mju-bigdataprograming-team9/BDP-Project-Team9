from pyspark.sql import SparkSession
import re
from collections import Counter
import pandas as pd

# Spark 세션 생성
spark = SparkSession.builder.appName("PolicyAnalysis_Spark").getOrCreate()

# HDFS 경로 설정
hdfs_directory_path = "hdfs:///user/maria_dev/crawler/merged_data_test/"
output_directory = "hdfs:///user/maria_dev/policy_articles/"

# 정책 키워드 정의
policy_labels = {
    1: ["부동산 매매", "실거래가", "거래량", "매도자", "매수자", "매매가 상승", "시장 안정"],
    2: ["전세", "월세", "임대차", "보증금", "임대료", "임차인", "전월세 상한제", "계약갱신청구권"],
    3: ["LTV", "DTI", "DSR", "대출 한도", "주택담보대출", "금리", "대출 규제", "대출 완화"],
    4: ["종부세", "취득세", "재산세", "양도소득세", "보유세", "세율", "공시가격", "세제 개편"]
}

def process_articles_with_rank(data, policy_labels):
    # 컬럼 이름 정리
    data = data.toDF(*[col_name.strip().replace("\r", "").replace("\n", "").strip() for col_name in data.columns])

    # 결측값 제거
    data = data.dropna(subset=["article_id", "content_text"])

    # 본문과 ID를 리스트로 수집
    articles = data.select("article_id", "content_text").rdd.collect()

    # 결과 리스트
    result_data = []

    # 각 기사를 순회하며 처리
    for article in articles:
        article_id, content_text = article.article_id, article.content_text

        # 본문에서 단어 추출
        article_words = re.findall(r'\w+', content_text)
        counter = Counter(article_words)

        # 정책별 키워드 빈도 계산
        policy_totals = {
            policy_id: sum(counter[keyword] for keyword in keywords)
            for policy_id, keywords in policy_labels.items()
        }

        # 가장 많이 등장한 정책 및 해당 Count
        max_policy_id = max(policy_totals, key=policy_totals.get)
        max_count = policy_totals[max_policy_id]
        policy_name = f"정책 {max_policy_id}" if max_count > 0 else "None"

        # 결과 추가
        result_data.append({
            "ID": article_id,
            "Policy": policy_name,
            "Count": max_count
        })

    # 결과를 DataFrame으로 변환
    df = pd.DataFrame(result_data)

    # Rank 계산
    df["Rank"] = df.groupby("Policy")["Count"].rank(method="dense", ascending=False).astype("Int64")

    # Rank를 문자열로 변환
    df["Rank"] = df["Rank"].astype(str)
    df.loc[df["Count"] == 0, "Rank"] = "None"  # Count가 0인 경우 Rank를 None으로 설정

    return df

def process_csv_files_with_rank(hdfs_directory_path, output_directory, policy_labels):
    # HDFS 파일 시스템 객체 생성
    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    input_path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_directory_path)
    
    # 입력 디렉토리 내의 모든 파일 목록 가져오기
    file_status_list = hadoop_fs.listStatus(input_path)
    file_paths = [file_status.getPath().toString() for file_status in file_status_list if file_status.getPath().toString().endswith('.csv')]

    # 각 파일 처리
    for file_path in file_paths:
        print(f"Processing file: {file_path}")

        # 파일 이름에서 식별자(월) 추출
        file_name = file_path.split("/")[-1]
        identifier = re.search(r"_(\d{6})\.csv", file_name)
        identifier = identifier.group(1) if identifier else "Unknown"

        # CSV 파일 읽기
        data = spark.read.option("header", "true") \
                         .option("multiLine", "true") \
                         .option("escape", '"') \
                         .csv(file_path)

        # 기사 처리
        result_df = process_articles_with_rank(data, policy_labels)
        result_df["Month"] = identifier

        # Spark DataFrame으로 변환
        spark_df = spark.createDataFrame(result_df)
        spark_df = spark_df.select("Month", "Policy", "ID", "Count", "Rank")

        # 파일별 저장
        output_file = f"{output_directory}/policy_article_{identifier}"
        spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_file)
        print(f"분석 결과가 {output_file}에 저장되었습니다.")

# 실행
process_csv_files_with_rank(hdfs_directory_path, output_directory, policy_labels)

# Spark 세션 종료
spark.stop()
