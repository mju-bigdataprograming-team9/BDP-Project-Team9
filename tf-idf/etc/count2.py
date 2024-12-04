# 파일 읽기
with open("word_count.txt", "r", encoding="utf-8") as file:
    lines = file.readlines()

# 단어 추출 및 숫자 제거
words = set()  # 중복 제거를 위해 set 사용
for line in lines:
    parts = line.strip().split()  # 줄별로 공백으로 분리
    for part in parts[1:]:  # 첫 번째 항목(번호)은 제외
        if not part.isdigit():  # 숫자가 아닌 경우만 추가
            words.add(part)

# 결과 정렬
sorted_words = sorted(words)  # 정렬된 단어 목록

# 결과를 파일로 저장
with open("result.txt", "w", encoding="utf-8") as file:
    for idx, word in enumerate(sorted_words, start=1):
        file.write(f"{idx}.{word}\n")

print("결과가 'result.txt' 파일에 저장되었습니다.")
