import pandas as pd

# 파일 읽기 및 데이터 처리
def process_policy_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # 데이터를 저장할 리스트 초기화
    data = []

    # 데이터 파싱
    for i in range(len(lines)):
        line = lines[i].strip()
        if line.startswith("파일:"):
            file_name = line.split(" ")[1]
        elif "정책" in line and "단어 발견" in line:
            policy_num = line.split(":")[0].strip()
            word_count = int(line.split(":")[1].split("개 단어")[0].strip())
            data.append((file_name, policy_num, word_count))
        elif line.startswith("최다 단어 정책:"):
            max_policy = line.split(":")[1].strip()
            # 최다 단어 정책 추가
            data.append((file_name, max_policy, '최다'))

    # 데이터프레임 생성
    df = pd.DataFrame(data, columns=['파일명', '정책', '단어 수'])

    # 숫자로 변환 가능한 값만 유지
    df_cleaned = df[df['단어 수'] != '최다']
    df_cleaned['단어 수'] = pd.to_numeric(df_cleaned['단어 수'])

    return df_cleaned


df_cleaned = process_policy_file("policy_5_results_with_counts.txt")

# 각 월별 합산 데이터프레임 생성
df_cleaned['월'] = df_cleaned['파일명'].str.extract(r'_(\d+)\.csv')[0]
monthly_summary = df_cleaned.groupby(['월', '정책'])['단어 수'].sum().unstack().fillna(0).reset_index()

# 중간값 계산
policy_medians = df_cleaned.groupby('정책')['단어 수'].median().to_dict()

# 상위 5 값 조건 계산
top5_thresholds = {policy: monthly_summary[policy].nlargest(5).min() for policy in ['정책 1', '정책 2', '정책 3', '정책 4']}

# 정책type 할당 함수
def assign_policy_type_with_top5(row, policy_medians, top5_thresholds):
    valid_policies = {}
    for policy, median in policy_medians.items():
        # 중간값보다 크고, 상위 5 값 안에 들어가는 경우만 유효
        if row[policy] > median and row[policy] >= top5_thresholds[policy]:
            valid_policies[policy] = row[policy]
    # 유효한 정책 중 가장 값이 큰 정책 선택
    if valid_policies:
        selected_policy = max(valid_policies, key=valid_policies.get)
        # 정책 3과 4를 통합
        if selected_policy in ['정책 3', '정책 4']:
            return '정책 3'
        return selected_policy
    return "None"

# 정책type 할당
monthly_summary['정책type'] = monthly_summary.apply(assign_policy_type_with_top5, axis=1, 
                                                    policy_medians=policy_medians, 
                                                    top5_thresholds=top5_thresholds)

# 결과 저장
output_file = "policy_summary_5_results.csv"
monthly_summary.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"분석 결과가 {output_file}에 저장되었습니다.")
