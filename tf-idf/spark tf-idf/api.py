from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from MeCab import Tagger

# FastAPI 인스턴스 생성
app = FastAPI()

# MeCab 초기화
tagger = Tagger("-d /usr/local/lib/mecab/dic/mecab-ko-dic -u /usr/local/lib/mecab/dic/mecab-ko-dic/user.dic")

# 요청 데이터 모델 정의
class TextRequest(BaseModel):
    text: str

# 형태소 분석 엔드포인트
@app.post("/analyze")
def analyze_text(request: TextRequest):
    try:
        if not request.text.strip():
            raise HTTPException(status_code=400, detail="Text cannot be empty")
        
        # MeCab 형태소 분석 수행
        tokens = tagger.parse(request.text).strip().splitlines()
        filtered_tokens = []
        for token in tokens:
            word_pos = token.split('\t')
            if len(word_pos) < 2:
                continue
            word, pos = word_pos[0], word_pos[1].split(',')[0]
            if pos in ['NNG', 'NNP', 'VV', 'VA']:  # 명사, 동사, 형용사만 포함
                filtered_tokens.append(word)
        
        return {"tokens": filtered_tokens}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# FastAPI 실행
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
