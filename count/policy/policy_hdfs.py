from hdfs import InsecureClient
import pandas as pd
import io
import re

# HDFS 설정
hdfs_url = 'http://sandbox-hdp.hortonworks.com:50070'  # WebHDFS URL
hdfs_directory_path = '/user/maria_dev/crawler/merged_data/'  # HDFS 디렉토리 경로
hdfs_client = InsecureClient(hdfs_url, user='maria_dev')

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

# HDFS 디렉토리에서 CSV 파일 가져오기
csv_files = hdfs_client.list(hdfs_directory_path)

# 모든 CSV 파일 처리 및 단어 카운트
policy_data = []
for file_name in csv_files:
    if file_name.endswith('.csv'):
        try:
            # HDFS에서 파일 읽기
            with hdfs_client.read(hdfs_directory_path + file_name) as reader:
                # 파일 내용을 메모리로 완전히 읽기
                file_content = reader.read()
                # 메모리 내용을 텍스트 스트림으로 변환
                text_stream = io.StringIO(file_content.decode('utf-8'))
                data = pd.read_csv(text_stream, error_bad_lines=False)  # 파일 읽기
            
            articles = data['content_text'].dropna()

            policy_totals = {policy_id: 0 for policy_id in policy_labels.keys()}
            for article in articles:
                for policy_id, policy_words in policy_labels.items():
                    for word in policy_words:
                        # 정규식을 사용하여 정책 키워드의 등장 횟수 계산
                        count = len(re.findall(re.escape(word), article))
                        policy_totals[policy_id] += count

            for policy_id, total in policy_totals.items():
                month = re.search(r'_(\d+)\.csv', file_name).group(1)
                policy_data.append((month, f"정책 {policy_id}", total))
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
            pass

# 데이터프레임 생성 및 월별 데이터 합산
df = pd.DataFrame(policy_data, columns=['월', '정책', '단어 수'])
monthly_summary = df.pivot_table(index='월', columns='정책', values='단어 수', aggfunc='sum').fillna(0).reset_index()

# 중간값 및 상위 10 값 계산
policy_medians = df.groupby('정책')['단어 수'].median().to_dict()
top10_thresholds = df.groupby('정책')['단어 수'].apply(lambda x: x.nlargest(10).min()).to_dict()

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
output_file = "test2_result.csv"
monthly_summary.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"분석 결과가 {output_file}에 저장되었습니다.")
