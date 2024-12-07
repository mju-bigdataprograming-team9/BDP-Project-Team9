from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class LoadArticles:
    def __init__(self):
        # SparkSession 생성 (애플리케이션에서 한 번만 생성됨)
        self.spark = SparkSession.builder \
            .appName("Policy Analysis") \
            .getOrCreate()

    def get_filtered_articles(self, target_month: str):
        """
        특정 월과 조건에 맞는 기사 데이터를 JSON 리스트로 반환
        """
        try:
            # hdfs 정책 데이터 경로
            hdfs_policy_path = "/user/maria_dev/test/policy_hdfs_result.csv"
            policy_df = self.spark.read.csv(
                hdfs_policy_path,
                header=True,
                multiLine=True,
                escape='"',
                quote='"',
                inferSchema=True
            )

            # 정책 타입 추출
            policy_type_row = policy_df.filter(col("월") == target_month).select("정책type").collect()
            print("len(policy_type_row): ", len(policy_type_row))
            if not policy_type_row:
                raise ValueError(f"No data found for month {target_month}")
            policy_type = policy_type_row[0][0]
            policy_type = policy_type.split()[-1]
            print("policy_type: ", policy_type)

            # HDFS 뉴스 데이터 경로
            hdfs_articles_path = f"/user/maria_dev/best_articles/month={target_month}/part-*.csv"
            articles_df = self.spark.read.csv(
                hdfs_articles_path,
                header=True,
                multiLine=True,
                escape='"',
                quote='"',
                inferSchema=True
            )

            # 조건에 맞는 ID 추출
            filtered_ids = articles_df.filter(
                (col("policy") == policy_type) & (col("rank") <= 3)
            ).select("ID").rdd.flatMap(lambda x: x).collect()
            filtered_ids = [str(id).zfill(10) for id in filtered_ids] 

            print("len(filtered_ids): ", len(filtered_ids))
            print("filtered_ids: ", filtered_ids)

            # HDFS 병합 데이터 경로
            hdfs_merged_path = f"/user/maria_dev/crawler/merged_data/merged_{target_month}.csv"
            merged_df = self.spark.read.csv(
                hdfs_merged_path,
                header=True,
                multiLine=True,
                escape='"',
                quote='"',
                inferSchema=True
            )
            print("merged_df.count(): ", merged_df.count())
            
            # ID 값에 맞는 row 필터링
            result_df = merged_df.filter(col("article_id").isin(filtered_ids))
            print("result_df.count(): ", result_df.count())

            # 결과를 JSON 리스트로 변환
            result = result_df.collect()
            print("len(result): ", len(result))
            return result

        except Exception as e:
            raise ValueError(f"Error during Spark job: {str(e)}")

    def stop(self):
        """
        SparkSession 종료
        """
        self.spark.stop()