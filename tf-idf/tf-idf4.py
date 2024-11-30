# vm에서 사용할 형태소 분석기 사용한 tf-idf Mecab 코드입니다.
import os
import pandas as pd
from MeCab import Tagger  # konlpy 대신 MeCab 직접 사용
from sklearn.feature_extraction.text import TfidfVectorizer

# 1. MeCab 형태소 분석기 초기화
tagger = Tagger('-d /usr/local/lib/mecab/dic/mecab-ko-dic')  # MeCab 경로 명시

# 2. 형태소 분석 및 불용어 제거 함수 정의
stopwords = [
    '주택', '아파트', '오피스텔', '집값', '가구', '서울', '분양', '부동산', '지구', 
    '수주', '역대', '사업', '서울시', '국토', '구역', '공시', '건설', '개발', '규제','스테이트',
    '지역', '도시', '가격', '한국', '빌라', '주거', '회장', '분기', '공사','영업'
]

def clean_and_tokenize(text):
    if not isinstance(text, str) or text.strip() == "":
        return []
    # MeCab으로 형태소 분석 수행
    tokens = tagger.parse(text).strip().splitlines()
    # 형태소와 품사를 분리하고 명사, 형용사, 동사만 포함
    filtered_tokens = []
    for token in tokens:
        # MeCab의 결과는 "형태소\t품사" 형태
        word_pos = token.split('\t')
        if len(word_pos) < 2:
            continue
        word, pos = word_pos[0], word_pos[1].split(',')[0]
        if pos in ['NNG', 'NNP', 'VV', 'VA'] and word not in stopwords:  # 명사, 동사, 형용사만 포함
            filtered_tokens.append(word)
    return filtered_tokens

# 3. 디렉토리 내 CSV 파일 읽기
directory_path = '../crawler/merged_data/'  # CSV 파일들이 있는 디렉토리 경로
csv_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.csv')]

# 4. 결과를 저장할 디렉토리 생성
result_directory = './mecab-result/'
os.makedirs(result_directory, exist_ok=True)  # 디렉토리 생성, 이미 있으면 무시

# 5. 모든 파일에 대해 처리 반복
for csv_file in csv_files:
    print(f"Processing file: {csv_file}")
    try:
        # CSV 파일 읽기
        df = pd.read_csv(csv_file, encoding='utf-8')
        
        # 텍스트 전처리 및 토큰화
        df['tokenized'] = df['title'].apply(lambda x: ' '.join(clean_and_tokenize(x)))

        # TF-IDF 벡터화
        cleaned_text_data = df['tokenized'].dropna()  # 결측치 제거
        tfidf_vectorizer = TfidfVectorizer(max_features=1000, stop_words=None)  # 최대 1000개 단어 사용
        tfidf_matrix = tfidf_vectorizer.fit_transform(cleaned_text_data)

        # TF-IDF 결과를 데이터프레임으로 변환
        columns = tfidf_vectorizer.get_feature_names()  # 단어 리스트
        tfidf_df = pd.DataFrame(
            tfidf_matrix.toarray(),
            columns=columns
        )

        # 상위 N개 단어 추출
        word_tfidf_mean = tfidf_df.mean(axis=0)
        top_n = 30
        top_tfidf_words = word_tfidf_mean.sort_values(ascending=False).head(top_n)

        # 결과 저장할 파일명 설정
        base_name = os.path.basename(csv_file)  # 파일명만 추출
        save_file_name = f"word_{base_name.split('_')[-1].split('.')[0]}.txt"  # word_202210.txt 형태
        save_file_path = os.path.join(result_directory, save_file_name)
        
        # 결과를 파일로 저장
        with open(save_file_path, 'w', encoding='utf-8') as f:
            for rank, (word, score) in enumerate(top_tfidf_words.items(), start=1):
                f.write(f"{rank}. {word}: {round(score, 4)}\n")

        print(f"Saved results to {save_file_path}")

    except Exception as e:
        print(f"Error processing file {csv_file}: {e}")
