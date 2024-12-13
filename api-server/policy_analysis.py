import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error


def read_and_merge_data(file_paths):
    """CSV 파일을 읽고 데이터 병합"""
    combined_data = pd.DataFrame()
    for file_path in file_paths:
        data = pd.read_csv(file_path, encoding='cp949', engine='python')
        combined_data = pd.concat([combined_data, data], ignore_index=True)
    return combined_data


def preprocess_data(data):
    """데이터 전처리: 거래금액 변환 및 면적당 가격 계산"""
    data['거래금액(만원)'] = pd.to_numeric(data['거래금액(만원)'].str.replace(',', ''), errors='coerce')
    data['㎡당 거래금액(만원)'] = data['거래금액(만원)'] / data['전용면적(㎡)']
    lower_bound = data['㎡당 거래금액(만원)'].quantile(0.01)
    upper_bound = data['㎡당 거래금액(만원)'].quantile(0.99)
    return data[(data['㎡당 거래금액(만원)'] >= lower_bound) & (data['㎡당 거래금액(만원)'] <= upper_bound)]


def calculate_monthly_change(data):
    """월별 변동률 계산"""
    monthly_avg_price = data.groupby('계약년월')['㎡당 거래금액(만원)'].mean().reset_index()
    monthly_avg_price['변동률(%)'] = monthly_avg_price['㎡당 거래금액(만원)'].pct_change() * 100
    monthly_avg_price['계약년월'] = pd.to_datetime(monthly_avg_price['계약년월'], format='%Y%m')
    return monthly_avg_price


def load_policy_data(policy_file_path):
    """정책 매핑 데이터 읽기"""
    policy_data = pd.read_csv(policy_file_path, encoding='utf-8')
    policy_data['월'] = policy_data['월'].astype(str)
    return policy_data


def merge_policy_data(changes, policy_data):
    """월별 변동률 데이터와 정책 매핑 데이터 병합"""
    changes['계약년월'] = pd.to_datetime(changes['계약년월'])
    changes['월'] = changes['계약년월'].dt.strftime('%Y%m')
    return pd.merge(changes, policy_data[['월', '정책type']], on='월', how='left')


def calculate_policy_effect(changes, periods=6):
    """정책별 변동률 평균 계산"""
    average_changes = []
    for policy in changes['정책type'].unique():
        if policy == 'None':
            continue
        policy_data = changes[changes['정책type'] == policy]
        for _, row in policy_data.iterrows():
            start_date = row['계약년월']
            future_data = changes[changes['계약년월'] > start_date]
            for period in range(1, periods + 1):
                end_date = start_date + pd.DateOffset(months=period)
                period_data = future_data[future_data['계약년월'] <= end_date]
                if not period_data.empty:
                    avg_change = period_data['변동률(%)'].mean()
                    average_changes.append({
                        '정책type': policy,
                        '기간(개월)': period,
                        '변동률 평균(%)': avg_change
                    })
    return pd.DataFrame(average_changes)


def analyze_policy_effect(changes, grouped_average_change, target_month):
    """정책 효과 분석 및 출력"""
    target_date = pd.to_datetime(target_month, format='%Y%m')
    # changes = changes[changes['정책type'] != 'None']
    # print("=======changes=======")
    # print(changes)
    changes['시차(개월)'] = (
        (target_date.year - changes['계약년월'].dt.year) * 12 +
        (target_date.month - changes['계약년월'].dt.month)
    )
    # print("=======changes2=======")
    # print(changes)
    policies_in_range = changes[
        (changes['정책type'] != 'None') &
        (changes['시차(개월)'] > 0) &
        (changes['시차(개월)'] <= 6)
    ]
    # print("=======policies_in_range.head()=======")
    # print(policies_in_range.head())
    policies_in_range = policies_in_range.merge(
        grouped_average_change,
        left_on=['정책type', '시차(개월)'],
        right_on=['정책type', '기간(개월)'],
        how='left'
    )
    # print("=======policies_in_range.head()2=======")
    # print(policies_in_range.head())
    # print(policies_in_range.head())
    policies_in_range['변동률_절대값'] = policies_in_range['변동률 평균(%)'].abs()
    policies_in_range = policies_in_range[['정책type', '월', '변동률 평균(%)', '변동률_절대값']].sort_values(by='변동률_절대값', ascending=False)

    # 첫 row의 정책type과 월을 튜플로 반환
    if not policies_in_range.empty:
        first_row = policies_in_range.iloc[0]
        return first_row['정책type'], first_row['월']
    return None, None


def calculate_error(actual, predicted):
    """RMSE 및 MAE 계산"""
    rmse = np.sqrt(mean_squared_error([actual], [predicted]))
    mae = mean_absolute_error([actual], [predicted])
    return rmse, mae