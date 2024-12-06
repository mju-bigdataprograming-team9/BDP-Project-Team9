# 네이버 뉴스 기사 크롤러

## 크롤링 정보 
    
| 필드명 | 필드 설명 |
|---------:|:---------|
| ARTICLE_ID | 뉴스 기사 ID |
| SECTION_ID | 뉴스 기사 카테고리. 예> 정치, 경제 등 |
| SUB_SECTION_ID | 뉴스 기사 하위 카테고리. 예 > 금융, 증권, 부동산 등 |
| NEWSPAPER_ID | 신문사 ID |
| NEWSPAPER_NAME | 신문사 명 |
| TITLE | 뉴스 기사 제목 |
| JOURNALIST_NAME | 기자 명 |
| CREATED_AT | 등록 일자 |
| UPDATED_AT | 수정 일자 |
| CONTENT_ORIGIN | 뉴스 기사 원문 (HTML 그대로) |
| CONTENT_TEXT | 뉴스 기사 텍스트 |
    

## 실행 방법

### 기사 카테고리 구분
```json
sections = {
    "부동산": {SECTION_ID: "101", SUB_SECTION_ID: "260"},
    "금융": {SECTION_ID: "101", SUB_SECTION_ID: "259"},
    "증권": {SECTION_ID: "101", SUB_SECTION_ID: "258"},
    "생활경제": {SECTION_ID: "101", SUB_SECTION_ID: "310"},
    "경제 일반": {SECTION_ID: "101", SUB_SECTION_ID: "263"},
    "행정": {SECTION_ID: "100", SUB_SECTION_ID: "266"},
    "정치일반": {SECTION_ID: "100", SUB_SECTION_ID: "269"},
    "산업/재계": {SECTION_ID: "101", SUB_SECTION_ID: "261"},
    "중기/벤처": {SECTION_ID: "101", SUB_SECTION_ID: "771"},
    "글로벌 경제": {SECTION_ID: "101", SUB_SECTION_ID: "262"},
    "대통령실": {SECTION_ID: "100", SUB_SECTION_ID: "264"},
    "국회/정당": {SECTION_ID: "100", SUB_SECTION_ID: "265"},
    "북한": {SECTION_ID: "100", SUB_SECTION_ID: "268"},
    "국방/외교": {SECTION_ID: "100", SUB_SECTION_ID: "267"},
}
```

### 기간 및 범위 지정 (./crawler.py 참고)
```python
# 실제 실행 코드
if __name__ == '__main__':
    section = sections.get("부동산") # 크롤링할 섹션 지정
    date = "20210208" # 시작일자 지정
    data_cursor='' # 예상치 못한 크롤링 중단 시, 이어하기 위해 네이버쪽에서 가져온 next 구분값
    while(data_cursor=='' or date >= '20201124'): # 기한 지정
```

### 백그라운드 실행 (centos 기준)
```bash
nohup python3.6 -u crawler.py > log.txt & 
```
### 로그 확인
```bash
tail -f log.txt
```