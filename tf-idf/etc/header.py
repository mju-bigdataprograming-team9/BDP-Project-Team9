import pandas as pd

# 기존 헤더 없는 CSV 파일 경로
input_file = '../crawler/merged_data/merged_202305.csv'

# 헤더를 추가할 컬럼 이름 정의
columns = ['article_id','section_id','sub_section_id','newspaper_id','newspaper_name','title','journalist_name','created_at','updated_at','content_origin','content_text']

# CSV 파일 읽기 (헤더가 없으므로 header=None 설정)
df = pd.read_csv(input_file, header=None, names=columns)

# 헤더가 추가된 CSV 파일 저장
output_file = './merged_202305.csv'
df.to_csv(output_file, index=False)

print(f"헤더가 추가된 파일이 {output_file}에 저장되었습니다.")
