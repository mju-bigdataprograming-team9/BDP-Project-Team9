from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from policy_analysis import *

class LoadArticles:
    def __init__(self):
        # SparkSession 생성 (애플리케이션에서 한 번만 생성됨)
        self.spark = SparkSession.builder \
            .appName("Policy Analysis") \
            .getOrCreate()

    def get_filtered_articles(self, target_month: str):
        file_paths = [
            '../system_code/201912_202011.csv', 
            '../system_code/202012_202111.csv', 
            '../system_code/202112_202211.csv',
            '../system_code/202212_202311.csv', 
            '../system_code/202312_202411.csv'
        ]
        policy_file_path = '../system_code/policy_mapped.csv'


        data = read_and_merge_data(file_paths)
        processed_data = preprocess_data(data)
        monthly_changes = calculate_monthly_change(processed_data)
        print("=======monthly_changes.head()=======")
        print(monthly_changes.head())
        policy_data = load_policy_data(policy_file_path)
        print("=======policy_data.head()=======")
        print(policy_data.head())
        combined_data = merge_policy_data(monthly_changes, policy_data)
        print("=======combined_data=======")
        print(combined_data)
        grouped_changes = calculate_policy_effect(combined_data)
        print("=======grouped_changes.head()=======")
        print(grouped_changes.head())
        policy_type, article_month = analyze_policy_effect(combined_data, grouped_changes, target_month)

        if article_month == None:
            print(f"기준 month: {target_month} 동안 영향을 미친 정책 데이터가 없습니다.")
            return None


        try:
            # hdfs 정책 데이터 경로
            # hdfs_policy_path = "/user/maria_dev/test/policy_hdfs_result.csv"
            # policy_df = self.spark.read.format("csv").option("header", "true").option("quote", '"').load(hdfs_policy_path)
            # policy_df = self.spark.read.csv(
            #     hdfs_policy_path,
            #     header=True,
            #     multiLine=True,
            #     escape='"',
            #     quote='"',
            #     inferSchema=True
            # )

            # 정책 타입 추출
            # policy_type_row = policy_df.filter(col("월") == target_month).select("정책type").collect()

            # target_month 이전 월 데이터에서 정책 타입이 None이 아닌 가장 최신 월의 정책 타입 추출
            # filtered_policy_df = policy_df.filter(
            #     (col("월") <= target_month) & (col("정책type").isNotNull())
            # ).orderBy(desc("월"))

            # policy_type_row = filtered_policy_df.select("정책type").limit(1).collect()
            # print("len(policy_type_row): ", len(policy_type_row))
            # if not policy_type_row:
            #     raise ValueError(f"No data found for month {target_month}")
            # policy_type = policy_type_row[0][0]
            # policy_type = policy_type.split()[-1]
            print("policy_type: ", policy_type)
            print("article_month: ", article_month)

            # HDFS 뉴스 데이터 경로
            hdfs_articles_path = f"/user/maria_dev/analysis_results/policy_articles/policy_article_{article_month}.csv"
            # hdfs_articles_path = f"hdfs://sandbox-hdp.hortonworks.com:50070/user/maria_dev/analysis_results/policy_articles/policy_article_{article_month}.csv"
            # hdfs_articles_path = f"/home/maria_dev/count/hadoop_policy/final/policy_articles/policy_article_{article_month}.csv"
            # articles_df = self.spark.read.format("csv").option("header", "true").option("quote", '"').load(hdfs_articles_path)
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
                (col("Policy") == policy_type) & (col("Rank") <= 3)
            ).select("ID").rdd.flatMap(lambda x: x).collect()
            filtered_ids = [str(id).zfill(10) for id in filtered_ids] 

            print("len(filtered_ids): ", len(filtered_ids))
            print("filtered_ids: ", filtered_ids)

            # HDFS 병합 데이터 경로
            hdfs_merged_path = f"/user/maria_dev/crawler/merged_data/merged_{article_month}.csv"
            # hdfs_merged_path = f"/home/maria_dev/crawler/merged_data/merged_{article_month}.csv"
            # merged_df = self.spark.read.format("csv").option("header", "true").option("quote", '"').load(hdfs_merged_path)
            merged_df = self.spark.read.csv(
                hdfs_merged_path,
                header=True,
                multiLine=True,
                escape='"',
                quote='"',
                # inferSchema=True
            )
            print("merged_df.count(): ", merged_df.count())
            # merged_df.printSchema()
            # merged_df.select("article_id").show(10, truncate=False)
            
            # ID 값에 맞는 row 필터링
            result_df = merged_df.filter(col("article_id").isin(filtered_ids))
            # result_df = merged_df.filter(col("article_id").cast("string").isin(['5037070']))
            print("result_df.count(): ", result_df.count())

            # 결과를 JSON 리스트로 변환
            # result_json = result_df.asDict().collect()
            # result_json = result_df.toJSON().collect()
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