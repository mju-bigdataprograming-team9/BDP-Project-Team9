
#제목에 있는 단어 가중치 2배, 본문은 1배
#전체 기사 수 대비 비율을 계산


import os
from collections import Counter
import pandas as pd
import re

# 정책 키워드
policy_labels = {
    1: [
        "부동산 매매", "실거래가", "거래량", "매도자", 
        "매수자", "매매가 상승", "시장 안정"
    ],  
    2: [
        "전세", "월세", "임대차", "보증금", 
        "임대료", "임차인", "전월세 상한제", "계약갱신청구권"
    ],  
    # 3: [
    #     "주택 공급", "신축", "재건축", "재개발", 
    #     "공공주택", "공급 부족"
    # ],  
    3: [
        "LTV", "DTI", "DSR", "대출 한도", 
        "주택담보대출", "금리", "대출 규제", "대출 완화"
    ],  
    4: [
        "종부세", "취득세", "재산세", "양도소득세", 
        "보유세", "세율", "공시가격", "세제 개편"
    ]  
}

# 디렉토리 경로 설정
directory_path = '/home/maria_dev/crawler/merged_data'  # CSV 파일들이 있는 디렉토리 경로
csv_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.csv')]

# 파일별 정책 단어 카운트 저장
file_policy_counts = {}
max_policy_counts = Counter()  # 최다 단어 정책별 카운트 저장

# 결과 저장을 위한 텍스트 리스트 초기화
output_lines = []

# 가중치 설정
title_weight = 2
body_weight = 1

# 모든 CSV 파일 처리
for file_path in csv_files:
    print(f"파일 처리 중: {file_path}")
    try:
        # 데이터 로드 및 결측값 제거
        data = pd.read_csv(file_path, error_bad_lines=False)
        titles = data['title'].dropna()
        articles = data['content_text'].dropna()

        # 단어 카운트 초기화
        file_counter = Counter()

        # 제목과 본문에서 키워드 Count (가중치 적용)
        for title in titles:
            title_words = re.findall(r'\w+', title)
            file_counter.update({word: count * title_weight for word, count in Counter(title_words).items()})
        
        for article in articles:
            body_words = re.findall(r'\w+', article)
            file_counter.update({word: count * body_weight for word, count in Counter(body_words).items()})

        # 정책별 단어 수 계산
        policy_totals = {policy_id: sum(file_counter[word] for word in policy_words)
                         for policy_id, policy_words in policy_labels.items()}

        # 변화율 계산
        policy_ratios = {policy_id: (count / len(articles)) * 100 for policy_id, count in policy_totals.items()}

        # 파일별 결과 저장
        file_policy_counts[os.path.basename(file_path)] = policy_totals

        # 최다 단어 정책 계산
        max_policy_id = max(policy_totals, key=policy_totals.get)
        max_policy_counts[max_policy_id] += 1

        # 텍스트 출력
        output_lines.append(f"파일: {os.path.basename(file_path)}")
        for policy_id, total in policy_totals.items():
            output_lines.append(f"  정책 {policy_id}: {total}개 단어 발견 ({policy_ratios[policy_id]:.2f}%)")
        output_lines.append("----------------------")
        output_lines.append(f"최다 단어 정책: 정책 {max_policy_id}\n")
    except Exception as e:
        print(f"파일 처리 중 오류 발생: {file_path}")
        print(f"오류 메시지: {e}")

# 파일별 결과 출력
print("\n파일별 정책별 단어 수 및 최다 단어 정책")
for file_name, policy_totals in file_policy_counts.items():
    print(f"\n파일: {file_name}")
    max_policy_id = max(policy_totals, key=policy_totals.get)
    for policy_id, total in policy_totals.items():
        print(f"  정책 {policy_id}: {total}개 단어 발견")
    print("----------------------")
    print(f"최다 단어 정책: 정책 {max_policy_id}\n")

# 최다 단어 정책별 카운트 출력
print("\n최다 단어 정책별 총합")
total_files = sum(max_policy_counts.values())
for policy_id, count in max_policy_counts.items():
    print(f"  정책 {policy_id}: {count}개 파일")
print(f"  총 파일 수: {total_files}개 파일")

# 결과를 txt 파일로 저장
output_file_path = "policy_5_results_with_counts.txt"
with open(output_file_path, "w", encoding="utf-8") as f:
    f.write("\n".join(output_lines))
    f.write("\n최다 단어 정책별 총합\n")
    for policy_id, count in max_policy_counts.items():
        f.write(f"정책 {policy_id}: {count}개 파일\n")
    f.write(f"총 파일 수: {total_files}개 파일\n")

print(f"결과가 '{output_file_path}' 파일에 저장되었습니다.")
