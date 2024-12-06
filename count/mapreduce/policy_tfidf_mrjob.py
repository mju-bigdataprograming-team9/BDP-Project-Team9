import warnings
from mrjob.job import MRJob
from sklearn.feature_extraction.text import TfidfVectorizer
from bs4 import BeautifulSoup
import re
import json
import csv


class PolicyTFIDFAnalysis(MRJob):
    def configure_args(self):
        super(PolicyTFIDFAnalysis, self).configure_args()
        self.add_file_arg('--policy_labels', help='Path to the JSON file containing policy labels.')

    def mapper_init(self):
        # 경고 무시 설정
        warnings.filterwarnings("ignore", category=UserWarning, module='sklearn')

        # 정책 라벨과 관련 단어 로드
        with open(self.options.policy_labels, 'r', encoding='utf-8') as f:
            self.policy_labels = json.load(f)

        # 모든 정책 단어의 리스트 생성
        self.all_policy_words = [word for words in self.policy_labels.values() for word in words]

        # TF-IDF Vectorizer 초기화 및 단어 학습
        self.tfidf_vectorizer = TfidfVectorizer(
            vocabulary=self.all_policy_words,
            tokenizer=lambda x: re.findall(r'\w+', x),
            stop_words=None
        )
        # Vectorizer를 fit (고정된 vocabulary 사용)
        self.tfidf_vectorizer.fit(self.all_policy_words)

    def mapper(self, _, line):
        try:
            # CSV 데이터를 파싱
            reader = csv.reader([line])
            for row in reader:
                if row[0] == "article_id":  # 헤더 스킵
                    return

                if len(row) < 11:  # content_text 필드 확인
                    return

                article_id, _, _, _, _, _, _, _, _, _, content_text = row

                # HTML 태그 제거
                content_text = BeautifulSoup(content_text, 'html.parser').get_text()

                # TF-IDF 계산
                if content_text.strip():  # content_text가 비어 있지 않을 경우
                    tfidf_matrix = self.tfidf_vectorizer.transform([content_text])
                    tfidf_scores = tfidf_matrix.toarray()[0]

                    # 정책별 TF-IDF 점수 계산
                    for policy_id, policy_words in self.policy_labels.items():
                        indices = [
                            self.tfidf_vectorizer.vocabulary_.get(word)
                            for word in policy_words if word in self.tfidf_vectorizer.vocabulary_
                        ]
                        # 점수를 계산하고, None은 무시
                        score = sum(tfidf_scores[idx] for idx in indices if idx is not None)
                        yield (policy_id, article_id), float(score)  # 숫자형으로 출력
        except Exception as e:
            self.increment_counter('errors', 'mapper_errors', 1)
            yield "Error", 0.0  # 오류가 발생하면 기본값 0.0 반환

    def reducer(self, key, values):
        try:
            # 모든 값을 float으로 변환 후 합산
            yield key, sum(float(value) for value in values)
        except Exception as e:
            self.increment_counter('errors', 'reducer_errors', 1)
            yield key, 0.0  # 오류 발생 시 기본값 반환


if __name__ == '__main__':
    PolicyTFIDFAnalysis.run()
