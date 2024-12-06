import os
import pandas as pd
import re
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

# 디렉토리 경로 설정
directory_path = '/home/maria_dev/crawler/merged_data'  # CSV 파일들이 있는 디렉토리 경로
csv_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.csv')]

# 파일별 정책 단어 카운트 저장
policy_data = []

# 가중치 설정
title_weight = 2
body_weight = 1

# 모든 CSV 파일 처리 및 단어 카운트
for file_path in csv_files:
    print(f"Processing file: {file_path}")
    try:
        # 데이터 로드 및 결측값 제거
        data = pd.read_csv(file_path, error_bad_lines=False)
        titles = data['title'].dropna()
        articles = data['content_text'].dropna()

        # 단어 카운트 초기화
        file_counter = Counter()

        # 제목과 본문에서 키워드 Count (단어 단위)
        for title in titles:
            title_words = re.findall(r'\w+', title)
            file_counter.update({word: count * title_weight for word, count in Counter(title_words).items()})

        for article in articles:
            body_words = re.findall(r'\w+', article)
            file_counter.update({word: count * body_weight for word, count in Counter(body_words).items()})

        # 정책별 단어 수 계산 (단어 단위)
        policy_totals = {policy_id: sum(file_counter[word] for word in policy_words)
                         for policy_id, policy_words in policy_labels.items()}

        # 파일별 단어 수 저장
        file_name = os.path.basename(file_path)
        month = re.search(r'_(\d+)\.csv', file_name).group(1)
        for policy_id, total in policy_totals.items():
            policy_data.append((month, f"정책 {policy_id}", total))

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# 데이터프레임 생성 및 월별 데이터 합산
df = pd.DataFrame(policy_data, columns=['월', '정책', '단어 수'])
monthly_summary = df.pivot_table(index='월', columns='정책', values='단어 수', aggfunc='sum').fillna(0).reset_index()

# 중간값 및 상위 10 값 계산
policy_medians = df.groupby('정책')['단어 수'].median().to_dict()
top10_thresholds = {policy: monthly_summary[policy].nlargest(10).min() for policy in monthly_summary.columns if policy != '월'}

# 정책type 할당 함수
def assign_policy_type_with_top10(row, policy_medians, top10_thresholds):
    valid_policies = {}
    for policy, median in policy_medians.items():
        if row[policy] > median and row[policy] >= top10_thresholds[policy]:
            valid_policies[policy] = row[policy]
    if valid_policies:
        return max(valid_policies, key=valid_policies.get)
    return "None"

# 정책type 할당
monthly_summary['정책type'] = monthly_summary.apply(assign_policy_type_with_top10, axis=1, 
                                                    policy_medians=policy_medians, 
                                                    top10_thresholds=top10_thresholds)

# 최종 결과 저장
output_file = "policy_5_merged_summary_result.csv"
monthly_summary.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"분석 결과가 {output_file}에 저장되었습니다.")
