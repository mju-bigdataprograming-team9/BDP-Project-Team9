import sys
import codecs
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# UTF-8 인코딩 설정
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

# Spark 세션 생성
spark = SparkSession.builder.appName("TF-IDF Analysis").getOrCreate()

# 로그 레벨 설정
spark.sparkContext.setLogLevel("ERROR")

# HDFS 경로 설정
hdfs_path = "hdfs:///user/maria_dev/scores/score_202409.txt"

# 데이터 로드
df = spark.read.text(hdfs_path)

# HDFS 경로상의 파일 출력
df.show(truncate=False)

# 데이터 파싱
def parse_lines(lines):
    articles = []
    current_article = {}
    for line in lines:
        line = line.strip()
        if line.startswith("ID:"):
            if current_article:
                articles.append(current_article)
            current_article = {"ID": None, "Title": None, "Policy1_TFIDF": None, "Policy2_TFIDF": None, "Policy3_TFIDF": None}
            match = re.match(r"ID: (\d+), 제목: (.+)", line)
            if match:
                current_article["ID"] = match.group(1)
                current_article["Title"] = match.group(2)
        elif "정책 1 TF-IDF 총합:" in line:
            match = re.match(r"정책 1 TF-IDF 총합: ([\d.]+)", line)
            if match:
                current_article["Policy1_TFIDF"] = float(match.group(1))
        elif "정책 2 TF-IDF 총합:" in line:
            match = re.match(r"정책 2 TF-IDF 총합: ([\d.]+)", line)
            if match:
                current_article["Policy2_TFIDF"] = float(match.group(1))
        elif "정책 3 TF-IDF 총합:" in line:
            match = re.match(r"정책 3 TF-IDF 총합: ([\d.]+)", line)
            if match:
                current_article["Policy3_TFIDF"] = float(match.group(1))
    if current_article:
        articles.append(current_article)
    return articles

lines = df.rdd.map(lambda row: row[0]).collect()
articles = parse_lines(lines)

# 명시적으로 스키마 정의
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("Policy1_TFIDF", FloatType(), True),
    StructField("Policy2_TFIDF", FloatType(), True),
    StructField("Policy3_TFIDF", FloatType(), True)
])

# DataFrame 생성
parsed_df = spark.createDataFrame(articles, schema)

# 누락된 값이 있는 행을 필터링
filtered_df = parsed_df.filter(col("ID").isNotNull() & col("Title").isNotNull())

# 정책별로 가장 높은 TF-IDF 값을 가진 기사 n개 가져오기
n = 3  # 가져올 기사 수

def get_top_n_articles(policy_col):
    window_spec = Window.orderBy(col(policy_col).desc())
    return filtered_df.filter(col(policy_col).isNotNull()) \
                      .withColumn("rank", row_number().over(window_spec)) \
                      .filter(col("rank") <= n) \
                      .select("ID", "Title", policy_col)

top_policy1 = get_top_n_articles("Policy1_TFIDF")
top_policy2 = get_top_n_articles("Policy2_TFIDF")
top_policy3 = get_top_n_articles("Policy3_TFIDF")

# 결과 출력
top_policy1.show(truncate=False)
top_policy2.show(truncate=False)
top_policy3.show(truncate=False)

# Spark 세션 종료
spark.stop()