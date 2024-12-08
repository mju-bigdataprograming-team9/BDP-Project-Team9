import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# 데이터 파일 경로
file1_path = '/home/maria_dev/system_code/201912_202011.csv'
file2_path = '/home/maria_dev/system_code/202012_202111.csv'
file3_path = '/home/maria_dev/system_code/202112_202211.csv'
file4_path = '/home/maria_dev/system_code/202212_202311.csv'
file5_path = '/home/maria_dev/system_code/202312_202411.csv'

# CSV 파일 로드
data_2019_2020 = pd.read_csv(file1_path, encoding='cp949', engine='python')
data_2020_2021 = pd.read_csv(file2_path, encoding='cp949', engine='python')
data_2021_2022 = pd.read_csv(file3_path, encoding='cp949', engine='python')
data_2022_2023 = pd.read_csv(file4_path, encoding='cp949', engine='python')
data_2023_2024 = pd.read_csv(file5_path, encoding='cp949', engine='python')
combined_data = pd.concat([data_2019_2020, data_2020_2021, data_2021_2022, data_2022_2023,
                          data_2023_2024], ignore_index=True)

# 거래금액(만원) 열 숫자 변환 (쉼표 제거 후 숫자로 변환)
combined_data['거래금액(만원)'] = pd.to_numeric(
    combined_data['거래금액(만원)'].str.replace(',', ''), errors='coerce'
)

# Function to calculate monthly average price per m2 and its change
def calculate_monthly_change(data):
    data['㎡당 거래금액(만원)'] = data['거래금액(만원)'] / data['전용면적(㎡)']
    lower_bound = data['㎡당 거래금액(만원)'].quantile(0.01)  # 하위 1%
    upper_bound = data['㎡당 거래금액(만원)'].quantile(0.99)  # 상위 1%
    filtered_data = data[
        (data['㎡당 거래금액(만원)'] >= lower_bound) &
        (data['㎡당 거래금액(만원)'] <= upper_bound)
    ]
    monthly_avg_price = filtered_data.groupby('계약년월')['㎡당 거래금액(만원)'].mean().reset_index()
    monthly_avg_price['변동률(%)'] = monthly_avg_price['㎡당 거래금액(만원)'].pct_change() * 100
    monthly_avg_price['계약년월'] = pd.to_datetime(monthly_avg_price['계약년월'], format='%Y%m')
    return monthly_avg_price

# 전체 데이터의 변동률 계산
overall_change = calculate_monthly_change(combined_data)
# 사용자 입력: 시작 날짜와 끝 날짜
while True:
    try:
        start_date = input("시작 날짜를 입력하세요 (예: 2020-01): ")
        end_date = input("끝 날짜를 입력하세요 (예: 2022-12): ")
        start_date = pd.to_datetime(start_date, format='%Y-%m')
        end_date = pd.to_datetime(end_date, format='%Y-%m')
        if start_date > end_date:
            print("시작 날짜는 끝 날짜보다 이전이어야 합니다. 다시 입력해주세요.")
            continue
        break
    except ValueError:
        print("올바른 형식으로 입력해주세요 (예: 2020-01).")

# 선택된 기간의 데이터 필터링
filtered_change = overall_change[
    (overall_change['계약년월'] >= start_date) & (overall_change['계약년월'] <= end_date)
]

# 선택된 기간 데이터 시각화
if filtered_change.empty:
    print("선택한 기간 내 데이터가 없습니다.")
else:
    plt.figure(figsize=(12, 6))
    plt.plot(filtered_change['계약년월'], filtered_change['변동률(%)'], marker='o', linestyle='-', color='blue', label='선택된 기간')
    plt.title(f'변동률(%) 변화 ({start_date.strftime("%Y-%m")} ~ {end_date.strftime("%Y-%m")})', fontsize=16)
    plt.xlabel('계약년월', fontsize=12)
    plt.ylabel('변동률(%)', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(fontsize=10)
    plt.tight_layout()
    plt.show()



