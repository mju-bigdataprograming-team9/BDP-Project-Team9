from typing import Union
import json

from fastapi import FastAPI, HTTPException
from load_articles import LoadArticles

app = FastAPI()

# SparkJob 인스턴스 생성 (앱 시작 시 한 번만 생성)
load_job = LoadArticles()


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/search/{yearmonth}")
async def search(yearmonth: int):
    """
    <div>
    <div>매매가 변동 원인을 나타내는 기사를 조회하는 API</div>
    <br />
    <div><strong>Path Parameters</strong></div>
    <div>----------</div>
    <div>yearmonth(str, YYYYMM):&nbsp;조회하고자 하는 매매가 변동 기간</div>
    <br />
    <div><strong>Returns</strong></div>
    <div>-------</div>
    <div>articles(json list): 원인을 나타내는 기사들을 json 리스트로 반환</div>
    </div>
    """

    try:
        result = load_job.get_filtered_articles(yearmonth)
        if not result:
            raise HTTPException(status_code=404, detail="No articles found for the given month.")

        # JSON 문자열을 Python 딕셔너리로 변환
        dict_result = []
        for row in result:
            dict_row = row.asDict()
            dict_row['article_url'] = f"https://n.news.naver.com/mnews/article/{dict_row.get('newspaper_id')}/{dict_row.get('article_id')}"
            dict_result.append(dict_row)

        return {"month": yearmonth, "articles": dict_result}
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/unpersist_dataframes")
async def unpersist_dataframes():
    """
    모든 DataFrame 캐시를 해제
    """
    try:
        # 명시적으로 캐시된 DataFrame을 해제
        for df in load_job.spark.sharedState.cacheManager.cachedData:
            df.unpersist(blocking=True)
        return {"message": "All DataFrame caches cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing DataFrame caches: {str(e)}")

@app.on_event("shutdown")
def shutdown_event():
    """
    애플리케이션 종료 시 SparkSession 종료
    """
    load_job.stop()