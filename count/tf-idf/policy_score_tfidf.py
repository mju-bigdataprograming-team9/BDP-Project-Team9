import os
import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import Counter

# 정책 라벨과 관련 단어 정의
policy_labels = {
    1: ["실거래가", "전매 제한", "경매", "주택 거래", "안정화 대책", "매각", 
        "부동산 매매", "매매가", "매물", "매수", "매입", "투기지역", 
        "실수요자", "주택구매", "거래절벽"],
    2: ["공공임대", "전월세 상한제", "금융규제", "디딤돌", "보증금", "임대차", 
        "전세", "월세", "전세대출", "임차인", "주택담보", "LTV", 
        "DTI", "DSR", "계약갱신"],
    3: ["분양가 상한", "주택공급", "택지개발촉진법", "과세", "분상", "재건축", 
        "초과이익환수제", "재개", "재시공", "인허가", "조세", "양도세", 
        "종합부동산세", "취득세", "부동산세제"]
}

# 정책별 단어 리스트 합치기
all_policy_words = [word for words in policy_labels.values() for word in words]

# TF-IDF Vectorizer 설정
tfidf_vectorizer = TfidfVectorizer(vocabulary=all_policy_words, tokenizer=lambda x: re.findall(r'\w+', x), stop_words=None)

# 디렉토리 경로 설정
directory_path = '../crawler/merged_data/'  # CSV 파일들이 있는 디렉토리 경로
csv_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.csv')]

# 파일별 정책 점수 저장
file_policy_scores = {}
max_policy_counts = Counter()  # 최다 단어 정책별 카운트 저장
output_lines = []

# 모든 CSV 파일 처리
for file_path in csv_files:
    print(f"파일 처리 중: {file_path}")
    try:
        # 데이터 로드 및 'content_text' 열 가져오기
        data = pd.read_csv(file_path, error_bad_lines=False)
        articles = data['content_text'].dropna()  # 결측값 제거

        # TF-IDF 계산
        tfidf_matrix = tfidf_vectorizer.fit_transform(articles)

        # 정책별 TF-IDF 점수 합산
        policy_totals = {}
        for policy_id, policy_words in policy_labels.items():
            indices = [tfidf_vectorizer.vocabulary_.get(word) for word in policy_words if word in tfidf_vectorizer.vocabulary_]
            if indices:
                policy_totals[policy_id] = tfidf_matrix[:, indices].sum()
            else:
                policy_totals[policy_id] = 0

        # 파일별 결과 저장
        file_policy_scores[os.path.basename(file_path)] = policy_totals

        # 최다 점수 정책 계산
        max_policy_id = max(policy_totals, key=policy_totals.get)
        max_policy_counts[max_policy_id] += 1  # 최다 정책 카운트 누적

        # 텍스트 파일에 저장할 내용 추가
        output_lines.append(f"파일: {os.path.basename(file_path)}")
        for policy_id, total in policy_totals.items():
            output_lines.append(f"  정책 {policy_id}: {total:.4f} 점수")
        output_lines.append("----------------------")
        output_lines.append(f"최다 점수 정책: 정책 {max_policy_id}\n")
    except Exception as e:
        print(f"파일 처리 중 오류 발생: {file_path}")
        print(f"오류 메시지: {e}")

# 파일별 결과 출력
print("\n파일별 정책별 TF-IDF 점수 및 최다 점수 정책")
for file_name, policy_totals in file_policy_scores.items():
    print(f"\n파일: {file_name}")
    max_policy_id = max(policy_totals, key=policy_totals.get)
    for policy_id, total in policy_totals.items():
        print(f"  정책 {policy_id}: {total:.4f} 점수")
    print("----------------------")
    print(f"최다 점수 정책: 정책 {max_policy_id}\n")

# 최다 단어 정책별 카운트 출력
print("\n최다 단어 정책별 총합")
total_files = sum(max_policy_counts.values())
for policy_id, count in max_policy_counts.items():
    print(f"  정책 {policy_id}: {count}개 파일")
print(f"  총 파일 수: {total_files}개 파일")

# 결과를 텍스트 파일로 저장
output_file_path = "policy_results_with_tfidf_scores.txt"
with open(output_file_path, "w", encoding="utf-8") as f:
    f.write("\n".join(output_lines))
    f.write("\n최다 단어 정책별 총합\n")
    for policy_id, count in max_policy_counts.items():
        f.write(f"정책 {policy_id}: {count}개 파일\n")
    f.write(f"총 파일 수: {total_files}개 파일\n")

print(f"결과가 '{output_file_path}' 파일에 저장되었습니다.")
