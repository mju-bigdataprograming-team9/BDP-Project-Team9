from hdfs import InsecureClient
import pandas as pd
import re
import io
from collections import Counter

# HDFS 설정
hdfs_url = 'http://sandbox-hdp.hortonworks.com:50070'  # WebHDFS URL
hdfs_directory_path = '/user/maria_dev/crawler/merged_data/'  # HDFS 디렉토리 경로
hdfs_client = InsecureClient(hdfs_url, user='maria_dev')

# 정책 키워드
policy_labels = {
    1: ["부동산 매매", "실거래가", "거래량", "매도자", "매수자", "매매가 상승", "시장 안정"],
    2: ["전세", "월세", "임대차", "보증금", "임대료", "임차인", "전월세 상한제", "계약갱신청구권"],
    3: ["LTV", "DTI", "DSR", "대출 한도", "주택담보대출", "금리", "대출 규제", "대출 완화"],
    4: ["종부세", "취득세", "재산세", "양도소득세", "보유세", "세율", "공시가격", "세제 개편"]
}

# 가중치 설정
title_weight = 2
body_weight = 1

# HDFS에서 CSV 파일 처리 및 단어 카운트
def process_hdfs_files(hdfs_directory_path):
    all_data = []
    csv_files = hdfs_client.list(hdfs_directory_path)

    for file_name in csv_files:
        if file_name.endswith('.csv'):
            try:
                # HDFS에서 파일 읽기
                with hdfs_client.read(hdfs_directory_path + file_name) as reader:
                    file_content = reader.read()
                    text_stream = io.StringIO(file_content.decode('utf-8'))
                    data = pd.read_csv(text_stream, error_bad_lines=False)

                # 제목과 본문 데이터 추출
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

                # 파일명에서 월 추출
                month = re.search(r'_(\d+)\.csv', file_name).group(1)

                # 데이터 저장
                for policy_id, count in policy_totals.items():
                    all_data.append((file_name, month, f"정책 {policy_id}", count))
            except Exception as e:
                print(f"Error processing {file_name}: {e}")
                continue
    return pd.DataFrame(all_data, columns=['파일명', '월', '정책', '단어 수'])

# 데이터 처리
df_cleaned = process_hdfs_files(hdfs_directory_path)

# 각 월별 합산 데이터프레임 생성
monthly_summary = df_cleaned.groupby(['월', '정책'])['단어 수'].sum().unstack().fillna(0).reset_index()

# 중간값 및 상위 5 값 계산
policy_medians = df_cleaned.groupby('정책')['단어 수'].median().to_dict()
top5_thresholds = {policy: monthly_summary[policy].nlargest(5).min() for policy in monthly_summary.columns if policy != '월'}

# 정책type 할당 함수
def assign_policy_type_with_top5(row, policy_medians, top5_thresholds):
    valid_policies = {}
    for policy, median in policy_medians.items():
        if row[policy] > median and row[policy] >= top5_thresholds[policy]:
            valid_policies[policy] = row[policy]
    if valid_policies:
        selected_policy = max(valid_policies, key=valid_policies.get)
        if selected_policy in ['정책 3', '정책 4']:
            return '정책 3'
        return selected_policy
    return "None"

# 정책type 할당
monthly_summary['정책type'] = monthly_summary.apply(assign_policy_type_with_top5, axis=1, 
                                                    policy_medians=policy_medians, 
                                                    top5_thresholds=top5_thresholds)

# 결과 저장
output_file = "policy_hdfs_summary_results.csv"
monthly_summary.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"분석 결과가 {output_file}에 저장되었습니다.")
