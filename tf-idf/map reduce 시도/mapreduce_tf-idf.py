from mrjob.job import MRJob
from mrjob.step import MRStep
from MeCab import Tagger
import math

class TFIDFMapReduce(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer_count_words
            ),
            MRStep(
                reducer_init=self.reducer_init,
                reducer=self.reducer_tfidf
            )
        ]

    def configure_args(self):
        super(TFIDFMapReduce, self).configure_args()
        self.add_passthru_arg('--stopwords', help='Path to stopwords file')

    def mapper_init(self):
        # MeCab 초기화
        mecab_dic_path = '-d /usr/local/lib/mecab/dic/mecab-ko-dic -u /usr/local/lib/mecab/dic/mecab-ko-dic/user.dic'
        self.tagger = Tagger(mecab_dic_path)

        # 불용어 로드
        stopwords_path = self.options.stopwords
        self.stopwords = set()
        if stopwords_path:
            with open(stopwords_path, 'r', encoding='utf-8') as f:
                self.stopwords = set(f.read().splitlines())

    def mapper(self, _, line):
        """문서 이름과 텍스트를 처리하고 토큰화."""
        file_name, text = line.split(',', 1)  # 파일 이름과 텍스트 분리
        tokens = self.clean_and_tokenize(text)
        for token in tokens:
            yield (file_name, token), 1

    def combiner(self, key, counts):
        """중복 키의 합계를 미리 계산."""
        yield key, sum(counts)

    def reducer_count_words(self, key, counts):
        """문서별 단어 등장 횟수 계산."""
        yield key[0], (key[1], sum(counts))  # file_name, (word, count)

    def reducer_init(self):
        """전체 문서 개수 초기화."""
        self.total_documents = 0

    def reducer_tfidf(self, file_name, word_counts):
        """TF-IDF 계산 및 정렬."""
        self.total_documents += 1  # 각 문서 처리 시 카운트
        word_counts = list(word_counts)
        total_word_count = sum(count for _, count in word_counts)

        # 각 단어가 등장한 문서 수 계산
        num_documents_with_word = {}
        for word, count in word_counts:
            num_documents_with_word[word] = num_documents_with_word.get(word, 0) + 1

        # TF-IDF 계산
        word_tfidf = {}
        for word, count in word_counts:
            tf = count / total_word_count
            idf = math.log((self.total_documents + 1) / (num_documents_with_word[word] + 1)) + 1
            word_tfidf[word] = tf * idf

        # TF-IDF 상위 50개 추출
        sorted_tfidf = sorted(word_tfidf.items(), key=lambda x: x[1], reverse=True)[:50]
        yield file_name, sorted_tfidf

    def clean_and_tokenize(self, text):
        """MeCab을 사용해 텍스트 토큰화 및 정리."""
        if not isinstance(text, str) or text.strip() == "":
            return []
        tokens = self.tagger.parse(text).strip().splitlines()
        filtered_tokens = []
        for token in tokens:
            word_pos = token.split('\t')
            if len(word_pos) < 2:
                continue
            word, pos = word_pos[0], word_pos[1].split(',')[0]
            if pos in ['NNG', 'NNP', 'VV', 'VA'] and word not in self.stopwords:
                filtered_tokens.append(word)
        return filtered_tokens

if __name__ == '__main__':
    TFIDFMapReduce.run()
    