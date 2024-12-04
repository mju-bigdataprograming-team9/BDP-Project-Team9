import os
from collections import Counter

# 1. mecab-result 디렉토리 경로
result_directory = './mecab-result/'

# 2. 모든 txt 파일 읽기
txt_files = [os.path.join(result_directory, file) for file in os.listdir(result_directory) if file.endswith('.txt')]

# 3. 단어 카운트를 위한 Counter 초기화
word_counter = Counter()

# 4. 각 파일에서 단어 추출 및 카운트
for txt_file in txt_files:
    with open(txt_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            # "순위. 단어: 점수" 형태에서 단어만 추출
            if '.' in line and ':' in line:  # '.'와 ':'가 모두 포함된 라인만 처리
                parts = line.split('.', 1)  # 첫 번째 '.'만 분리
                if len(parts) > 1:
                    word_section = parts[1].split(':', 1)  # 첫 번째 ':'만 분리
                    if len(word_section) > 1:
                        word = word_section[0].strip()  # 단어만 추출
                        word_counter[word] += 1

# 5. 단어별 출현 횟수 출력
output_lines = []
for rank, (word, count) in enumerate(word_counter.most_common(), start=1):
    line = f"{rank}. {word} {count}"
    print(line)  # 터미널 출력
    output_lines.append(line)

# 6. 결과를 word_count.txt로 저장
output_file_path = os.path.join(result_directory, 'word_count.txt')
with open(output_file_path, 'w', encoding='utf-8') as f:
    f.write("\n".join(output_lines))

print(f"Word count results saved to {output_file_path}")
