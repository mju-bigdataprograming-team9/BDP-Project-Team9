import os
import pandas as pd
from MeCab import Tagger
from sklearn.feature_extraction.text import TfidfVectorizer

# 1. MeCab 형태소 분석기 초기화
tagger = Tagger('-d /usr/local/lib/mecab/dic/mecab-ko-dic -u /usr/local/lib/mecab/dic/mecab-ko-dic/user.dic')  # MeCab 경로 명시

# 정책 라벨과 관련 단어 정의
policy_labels = {
    1: [
        "가격", "거래", "거래량", "경매", "계약", "고가", "매각", "매매", "매물", "매수", "매입", 
        "시세", "시장", "실거", "실수요자", "주거", "주택", "집값", "부동산"
    ],
    2: [
        "공공임대", "금리", "금융", "깡통", "디딤돌", "보증금", "임대", "임대인", "전세", 
        "전셋값", "월세", "중도금", "취득세"
    ],
    3: [
        "분양", "공급", "건설", "건축", "과세", "국토", "규제", "민간", "분상", "재건", 
        "재건축", "재개", "재시공", "인허가", "신축", "조세", "택지", "토지", "양도세", "허가", 
        "주택공급"
    ]
}

# 데이터 디렉토리 경로
directory_path = "../crawler/merged_data/"
output_directory = "./scores/"
os.makedirs(output_directory, exist_ok=True)  # 결과 저장 디렉토리 생성

# 2. MeCab 기반 정책 단어 필터링 함수
def filter_policy_words_with_mecab(text, word_set):
    if not isinstance(text, str) or text.strip() == "":
        return []
    # MeCab으로 형태소 분석 수행
    tokens = tagger.parse(text).strip().splitlines()
    filtered_words = []
    for token in tokens:
        word_pos = token.split('\t')
        if len(word_pos) < 2:
            continue
        word = word_pos[0]
        if word in word_set:  # 정책 단어와 매칭
            filtered_words.append(word)
    return filtered_words

# 3. 디렉토리 내부 모든 CSV 파일 처리
csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv')]

for csv_file in csv_files:
    try:
        file_path = os.path.join(directory_path, csv_file)
        print(f"Processing file: {file_path}")
        
        # 데이터 로드 및 'content_text' 열 가져오기
        data = pd.read_csv(file_path)
        articles = data[['article_id', 'title', 'content_text']].dropna()  # 결측값 제거
        
        # 각 정책에 대해 TF-IDF 계산
        policy_scores = {}
        total_tfidf_sums = {}  # 전체 파일 기준 총합 계산용
        for policy_id, policy_words in policy_labels.items():
            word_set = set(policy_words)
            
            # 각 기사에서 해당 정책 단어만 추출
            articles[f'filtered_policy_{policy_id}'] = articles['content_text'].apply(
                lambda x: " ".join(filter_policy_words_with_mecab(x, word_set)) or "없음"  # 정책 단어 없으면 기본값 설정
            )
            
            # TF-IDF 계산
            tfidf_vectorizer = TfidfVectorizer()
            tfidf_matrix = tfidf_vectorizer.fit_transform(articles[f'filtered_policy_{policy_id}'])
            
            # 단어별 TF-IDF 점수 추출 및 총합 계산
            tfidf_scores = pd.DataFrame(tfidf_matrix.toarray(), columns=tfidf_vectorizer.get_feature_names())
            tfidf_scores['total_tfidf'] = tfidf_scores.sum(axis=1).fillna(0)  # NaN을 0으로 대체
            articles[f'total_tfidf_policy_{policy_id}'] = tfidf_scores['total_tfidf']
            
            # 총합 기록
            policy_scores[policy_id] = articles[f'total_tfidf_policy_{policy_id}']
            
            # 전체 파일 기준 정책별 총합 계산
            total_tfidf_sums[policy_id] = tfidf_scores['total_tfidf'].sum()

        # NaN 값을 0으로 대체 (추가적인 안전 처리)
        for policy_id in policy_scores:
            articles[f'total_tfidf_policy_{policy_id}'] = articles[f'total_tfidf_policy_{policy_id}'].fillna(0)
        
        # 결과 저장 파일 이름 설정
        base_name = os.path.splitext(csv_file)[0]  # 파일명에서 확장자 제거
        save_file_name = f"score_{base_name.split('_')[-1]}.txt"  # score_YYYYMM.txt 형태
        save_file_path = os.path.join(output_directory, save_file_name)

        # 결과 저장
        with open(save_file_path, 'w', encoding='utf-8') as f:
            for _, row in articles.iterrows():
                f.write(f"ID: {row['article_id']}, 제목: {row['title']}\n")
                for policy_id, scores in policy_scores.items():
                    f.write(f"  정책 {policy_id} TF-IDF 총합: {scores[row.name]:.4f}\n")
                f.write("\n")
            
            # 전체 파일 기준 정책별 TF-IDF 총합 출력
            f.write("전체 파일 기준 정책별 TF-IDF 총합:\n")
            for policy_id, total_sum in total_tfidf_sums.items():
                f.write(f"  정책 {policy_id} 총합 TF-IDF: {total_sum:.4f}\n")
        
        print(f"Saved results to {save_file_path}")
    
    except Exception as e:
        print(f"Error processing file {csv_file}: {e}")
