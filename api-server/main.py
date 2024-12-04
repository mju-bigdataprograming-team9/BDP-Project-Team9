from typing import Union

from fastapi import FastAPI

app = FastAPI()


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
    <div>yearmonth(str):&nbsp;조회하고자 하는 매매가 변동 기간</div>
    <br />
    <div><strong>Returns</strong></div>
    <div>-------</div>
    <div>articles(json list): 원인을 나타내는 기사들을 json 리스트로 반환</div>
    </div>
    """

    articles = []
    return { "articles": articles }