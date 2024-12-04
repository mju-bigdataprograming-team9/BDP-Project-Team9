import requests
import re
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat_ws
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, NGram

# 1. Spark 세션 초기화
spark = SparkSession.builder \
    .appName("REST API Preprocessing with MeCab") \
    .getOrCreate()

# 2. REST API 호출 함수 정의
def call_mecab_api(text):
    """REST API를 통해 MeCab 형태소 분석 수행."""
    api_url = "http://localhost:8000/analyze"  # MeCab REST API 주소
    if not isinstance(text, str) or text.strip() == "":
        return []
    try:
        response = requests.post(api_url, json={"text": text})
        if response.status_code == 200:
            return response.json()["tokens"]
        else:
            return []
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {e}")
        return []

# UDF 등록
call_mecab_udf = udf(call_mecab_api, ArrayType(StringType()))

# 3. 데이터 디렉토리 설정
directory_path = "hdfs:///user/maria_dev/crawler/merged_data/"  # 디렉토리 경로
result_directory = "hdfs:///user/maria_dev/mecab-result-spark/"

# HDFS 디렉토리 내 파일 목록 가져오기
file_list = subprocess.run(
    ["hdfs", "dfs", "-ls", directory_path],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    universal_newlines=True
)

file_paths = [line.split()[-1] for line in file_list.stdout.splitlines() if line.strip().endswith(".csv")]

# 디렉토리 내 모든 파일 처리
for file_path in file_paths:
    print(f"Processing file: {file_path}")
    
    # HDFS에서 CSV 파일 읽기
    df = spark.read.csv(file_path, header=True, encoding="utf-8")
    
    # REST API를 활용한 형태소 분석
    df = df.withColumn("tokenized", call_mecab_udf(col("title")))

    # 배열 데이터를 문자열로 변환 (공백으로 구분)
    df = df.withColumn("tokenized_str", concat_ws(" ", col("tokenized")))

    # Tokenizer로 문자열 데이터를 토큰화
    tokenizer = Tokenizer(inputCol="tokenized_str", outputCol="words")
    words_data = tokenizer.transform(df)
    
    # N-gram 생성 (2-gram)
    ngram = NGram(n=2, inputCol="words", outputCol="ngrams")
    ngram_data = ngram.transform(words_data)

    # N-gram 데이터를 기반으로 TF-IDF 벡터화
    hashing_tf = HashingTF(inputCol="ngrams", outputCol="raw_features", numFeatures=1000)
    featurized_data = hashing_tf.transform(ngram_data)
    
    idf = IDF(inputCol="raw_features", outputCol="features")
    idf_model = idf.fit(featurized_data)
    rescaled_data = idf_model.transform(featurized_data)
    
    # 상위 단어 추출
    tfidf_scores = rescaled_data.select("words", "features").collect()
    tfidf_dict = {word: score for row in tfidf_scores for word, score in zip(row["words"], row["features"].toArray())}
    sorted_tfidf = sorted(tfidf_dict.items(), key=lambda x: x[1], reverse=True)[:50]
    
    # 상위 단어 및 점수를 DataFrame으로 변환
    from pyspark.sql import Row

    # numpy.float64를 Python float으로 변환
    rows = [Row(word=word, score=float(score)) for word, score in sorted_tfidf]
    top_words_df = spark.createDataFrame(rows)
    
    # 파일 이름 추출 및 결과 저장
    input_file_name = file_path.split("/")[-1]  # 예: "article_20201124.csv"
    date_match = re.search(r"\d{8}", input_file_name)
    if date_match:
        date_str = date_match.group(0)
        output_file_name = f"word_{date_str}.csv"
    else:
        output_file_name = f"word_{input_file_name}"
    
    # 결과 저장 경로
    result_path = f"{result_directory}/{output_file_name}"
    
    # 단일 파일로 저장하기 위해 데이터 병합 및 저장
    temp_output_dir = f"{result_directory}/temp_output"  # 임시 디렉토리
    top_words_df.coalesce(1).write.csv(temp_output_dir, header=True, mode="overwrite")

    # HDFS에서 파일 이름 변경
    saved_file_path = f"{temp_output_dir}/part-00000*.csv"
    final_file_path = f"{result_directory}/{output_file_name}"
    subprocess.run(["hdfs", "dfs", "-mv", saved_file_path, final_file_path])
    
    print(f"TF-IDF results saved to: {final_file_path}")
