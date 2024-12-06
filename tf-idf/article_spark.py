from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window
import re

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("MonthlyTopArticlesByPolicy") \
    .getOrCreate()

# 데이터 디렉토리 경로
input_directory = "hdfs:///user/maria_dev/scores/"

# HDFS에서 모든 TXT 파일 읽기
files_rdd = spark.sparkContext.wholeTextFiles(input_directory)

# 파일 내용 파싱 함수
def parse_file(file):
    file_name, content = file
    articles = []
    current_article = {}
    
    # 파일 이름에서 월 추출
    match = re.search(r"score_(\d{6})", file_name)
    month = match.group(1) if match else "unknown"

    for line in content.splitlines():
        line = line.strip()

        if "정책별 TF-IDF 총합:" in line:
            continue

        if line.startswith("ID:"):
            if current_article:
                articles.append(current_article)
            current_article = {"file": file_name, "month": month}
            parts = line.split(",")
            current_article["ID"] = parts[0].split(":")[1].strip()
            current_article["제목"] = parts[1].split(":")[1].strip()

        elif "TF-IDF 총합" in line:
            try:
                parts = line.split(":")
                policy_name = parts[0].strip()
                tfidf_value = float(parts[-1].strip())
                current_article[policy_name] = tfidf_value
            except ValueError:
                pass

    if current_article:
        articles.append(current_article)
    return articles

# 각 파일 파싱
parsed_rdd = files_rdd.flatMap(parse_file)

# 디버깅: parsed_rdd 내용 확인
parsed_data = parsed_rdd.take(10)
print("Parsed RDD Data:")
for item in parsed_data:
    print(item)

# 정책별 TF-IDF 값과 기사를 추출
def map_policy_to_number(policy_name):
    mapping = {
        "정책 1 TF-IDF 총합": 1,
        "정책 2 TF-IDF 총합": 2,
        "정책 3 TF-IDF 총합": 3
    }
    return mapping.get(policy_name, 0)

def extract_policy_articles(article):
    results = []
    policies = ["정책 1 TF-IDF 총합", "정책 2 TF-IDF 총합", "정책 3 TF-IDF 총합"]
    for policy in policies:
        if policy in article:
            policy_number = map_policy_to_number(policy)
            results.append((article["month"], policy_number, article["ID"], article["제목"], article[policy]))
    return results

policy_rdd = parsed_rdd.flatMap(extract_policy_articles)

# 디버깅: policy_rdd 내용 확인
policy_data = policy_rdd.take(10)
print("Policy RDD Data:")
for item in policy_data:
    print(item)

# DataFrame 변환
columns = ["month", "policy", "ID", "제목", "TF-IDF"]
policy_df = spark.createDataFrame(policy_rdd, schema=columns)

# 각 월별, 정책별로 TF-IDF 상위 3개 기사 선택
window_spec = Window.partitionBy("month", "policy").orderBy(col("TF-IDF").desc())
ranked_df = policy_df.withColumn("rank", rank().over(window_spec))

top_articles_df = ranked_df.filter(col("rank") <= 3)

# 월별 데이터 저장 경로
output_hdfs_path = "hdfs:///user/maria_dev/best_articles/"

# 월별 데이터를 하나의 파일로 저장
unique_months = [row["month"] for row in top_articles_df.select("month").distinct().collect()]

for month in unique_months:
    # 특정 월의 데이터 필터링
    month_df = top_articles_df.filter(top_articles_df["month"] == month)

    # 해당 월 데이터를 하나의 파일로 저장
    month_output_path = f"{output_hdfs_path}/month={month}"
    month_df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(month_output_path)

print(f"Data has been saved to {output_hdfs_path}")
