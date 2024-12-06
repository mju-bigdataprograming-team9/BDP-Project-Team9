import pandas as pd

# 파일 경로
file_path = "policy_results_with_tfidf_scores.txt"

# 파일 읽기 및 데이터 처리
def process_policy_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # 데이터를 저장할 리스트 초기화
    data = []

    # 데이터 파싱
    for line in lines:
        line = line.strip()
        if line.startswith("파일:"):
            file_name = line.split(" ")[1]
        elif "정책" in line and "점수" in line and not line.startswith("최다 점수 정책:"):
            # 점수 데이터 처리
            policy_num = line.split(":")[0].strip()
            score = float(line.split(":")[1].split("점수")[0].strip())
            data.append((file_name, policy_num, score))
        elif line.startswith("최다 점수 정책:"):
            # 최다 점수 정책은 별도로 처리하지 않음
            continue

    # 데이터프레임 생성
    df = pd.DataFrame(data, columns=['파일명', '정책', '점수'])
    return df


# 데이터 처리
df_cleaned = process_policy_file(file_path)

# 각 월별 합산 데이터프레임 생성
df_cleaned['월'] = df_cleaned['파일명'].str.extract(r'_(\d+)\.csv')[0]
monthly_summary = df_cleaned.groupby(['월', '정책'])['점수'].sum().unstack().fillna(0).reset_index()

# 중간값 계산
policy_medians = df_cleaned.groupby('정책')['점수'].median().to_dict()

# 상위 10 값 조건 계산
top10_thresholds = {policy: monthly_summary[policy].nlargest(10).min() for policy in ['정책 1', '정책 2', '정책 3']}

# 정책type 할당 함수
def assign_policy_type_with_top10(row, policy_medians, top10_thresholds):
    valid_policies = {}
    for policy, median in policy_medians.items():
        # 중간값보다 크고, 상위 10 값 안에 들어가는 경우만 유효
        if row[policy] > median and row[policy] >= top10_thresholds[policy]:
            valid_policies[policy] = row[policy]
    # 유효한 정책 중 가장 점수가 큰 정책 선택
    if valid_policies:
        return max(valid_policies, key=valid_policies.get)
    return "None"

# 정책type 할당
monthly_summary['정책type'] = monthly_summary.apply(assign_policy_type_with_top10, axis=1, 
                                                    policy_medians=policy_medians, 
                                                    top10_thresholds=top10_thresholds)

# 결과 저장
output_file = "policy_summary_results_with_tfidf.csv"
monthly_summary.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"분석 결과가 {output_file}에 저장되었습니다.")
