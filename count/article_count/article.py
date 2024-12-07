from hdfs import InsecureClient
import pandas as pd
import re
import io
import os
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

# 출력 폴더 설정
output_directory = "policy_articles_2"
os.makedirs(output_directory, exist_ok=True)

# HDFS에서 CSV 파일 처리 및 기사별 단어 카운트
def process_hdfs_articles(hdfs_directory_path):
    csv_files = hdfs_client.list(hdfs_directory_path)

    for file_name in csv_files:
        if file_name.endswith('.csv'):
            try:
                # HDFS에서 파일 읽기
                with hdfs_client.read(hdfs_directory_path + file_name) as reader:
                    file_content = reader.read()
                    text_stream = io.StringIO(file_content.decode('utf-8'))
                    data = pd.read_csv(text_stream, error_bad_lines=False)

                # 유효한 기사만 필터링 (필수 컬럼이 없거나 비어 있는 행 제거)
                required_columns = ['article_id', 'content_text']  # 필수 컬럼
                data = data.dropna(subset=required_columns)  # 필수 컬럼에 NaN 값이 있는 행 제거

                # 형식이 올바르지 않은 기사 제거
                data = data[data['content_text'].str.contains(r'[가-힣]', na=False)]  # 한글이 포함되지 않은 기사 제거

                # 기사별 데이터 초기화
                article_data = []

                # 파일명에서 월(Month) 추출
                month_match = re.search(r'_(\d{6})\.csv', file_name)
                if month_match:
                    month = month_match.group(1)
                else:
                    print(f"파일명에서 Month를 추출할 수 없습니다: {file_name}")
                    continue

                for _, row in data.iterrows():
                    article_id = row.get('article_id')
                    content_text = row.get('content_text', '')

                    # 단어 카운트
                    article_counter = Counter()

                    if pd.notna(content_text):
                        body_words = re.findall(r'\w+', content_text)
                        article_counter.update(body_words)

                    # 정책별 단어 수 계산
                    policy_totals = {policy_id: sum(article_counter[word] for word in policy_words)
                                     for policy_id, policy_words in policy_labels.items()}

                    # 가장 많은 키워드를 포함하는 정책 타입 선택
                    max_policy = max(policy_totals, key=policy_totals.get)
                    max_count = policy_totals[max_policy]

                    # 모든 정책 타입의 단어 수가 0인 경우 None 처리
                    if max_count == 0:
                        article_data.append((month, "None", article_id, 0, "None"))
                    else:
                        article_data.append((month, f"정책 {max_policy}", article_id, max_count, "None"))

                # 기사별 데이터프레임 생성
                df = pd.DataFrame(article_data, columns=['Month', 'Policy', 'ID', 'Count', 'Rank'])

                # 정책별로 Rank 계산
                for policy in df['Policy'].unique():
                    if policy != "None":
                        policy_df = df[df['Policy'] == policy]
                        df.loc[policy_df.index, 'Rank'] = policy_df['Count'].rank(method='dense', ascending=False).astype('Int64')

                # 월별 CSV 파일 저장
                output_file = os.path.join(output_directory, f"policy_article_{month}.csv")
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"월별 분석 결과가 {output_file}에 저장되었습니다.")

            except Exception as e:
                print(f"Error processing {file_name}: {e}")
                continue

# 기사별 데이터 처리
process_hdfs_articles(hdfs_directory_path)

