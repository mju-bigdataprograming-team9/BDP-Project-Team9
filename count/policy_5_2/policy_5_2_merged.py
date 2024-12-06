import os
import pandas as pd
import re
from collections import Counter

# 정책 키워드 정의
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

# 모든 CSV 파일 처리 및 단어 카운트
policy_data = []
max_policy_counts = Counter()

# 모든 CSV 파일 처리
for file_path in csv_files:
    print(f"Processing file: {file_path}")
    try:
        # 데이터 로드 및 결측값 제거
        data = pd.read_csv(file_path, error_bad_lines=False)
        titles = data['title'].dropna()
        articles = data['content_text'].dropna()

        # 단어 카운트 초기화
        policy_totals = {policy_id: 0 for policy_id in policy_labels.keys()}

        # 제목과 본문에서 키워드 Count (구 단위 포함)
        for title in titles:
            for policy_id, policy_words in policy_labels.items():
                for word in policy_words:
                    count = len(re.findall(re.escape(word), title))
                    policy_totals[policy_id] += count * 2  # 제목은 가중치 2배
        
        for article in articles:
            for policy_id, policy_words in policy_labels.items():
                for word in policy_words:
                    count = len(re.findall(re.escape(word), article))
                    policy_totals[policy_id] += count  # 본문은 가중치 1배

        # 파일별 단어 수 저장
        file_name = os.path.basename(file_path)
        month = re.search(r'_(\d+)\.csv', file_name).group(1)
        for policy_id, total in policy_totals.items():
            policy_data.append((month, f"정책 {policy_id}", total))

        # 최다 단어 정책 계산
        max_policy_id = max(policy_totals, key=policy_totals.get)
        max_policy_counts[max_policy_id] += 1

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# 데이터프레임 생성 및 월별 데이터 합산
df = pd.DataFrame(policy_data, columns=['월', '정책', '단어 수'])
monthly_summary = df.pivot_table(index='월', columns='정책', values='단어 수', aggfunc='sum').fillna(0).reset_index()

# 중간값 및 상위 5 값 계산
policy_medians = df.groupby('정책')['단어 수'].median().to_dict()
top5_thresholds = {policy: monthly_summary[policy].nlargest(5).min() for policy in df['정책'].unique()}

# 정책type 할당 함수
def assign_policy_type_with_top5(row, policy_medians, top5_thresholds):
    valid_policies = {}
    for policy, median in policy_medians.items():
        if row[policy] > median and row[policy] >= top5_thresholds[policy]:
            valid_policies[policy] = row[policy]
    if valid_policies:
        return max(valid_policies, key=valid_policies.get)
    return "None"

# 정책type 할당
monthly_summary['정책type'] = monthly_summary.apply(assign_policy_type_with_top5, axis=1, 
                                                    policy_medians=policy_medians, 
                                                    top5_thresholds=top5_thresholds)

# 최다 단어 정책별 카운트 출력
print("\n최다 단어 정책별 총합")
total_files = sum(max_policy_counts.values())
for policy_id, count in max_policy_counts.items():
    print(f"  정책 {policy_id}: {count}개 파일")
print(f"  총 파일 수: {total_files}개 파일")

# 결과 저장
output_file = "policy_summary_5_2_results.csv"
monthly_summary.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"분석 결과가 {output_file}에 저장되었습니다.")
