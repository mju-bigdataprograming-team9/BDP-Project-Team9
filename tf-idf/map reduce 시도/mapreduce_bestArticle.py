import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class TopArticlesByPolicy(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.map_extract_articles,
                reducer=self.reduce_group_by_policy
            ),
            MRStep(
                reducer=self.reduce_select_top3
            )
        ]

    def mapper_init(self):
        self.current_file_name = os.environ.get('map_input_file', 'unknown_file')
        self.current_file_name = os.path.basename(self.current_file_name)

    def map_extract_articles(self, _, line):
        line = line.strip()
        if line.startswith("ID:"):
            current_id = line.split(",")[0].split(":")[1].strip()
            current_title = line.split(",")[1].split(":")[1].strip()
            self.current_article = {"ID": current_id, "title": current_title}
        elif "TF-IDF Total" in line:
            try:
                parts = line.split(":")
                policy_name = parts[0].strip()
                tfidf_value = float(parts[-1].strip())
                self.current_article[policy_name] = tfidf_value
            except ValueError:
                pass
        elif not line and hasattr(self, "current_article"):
            max_policy, max_value = max(
                [("Policy 1 TF-IDF Total", self.current_article.get("Policy 1 TF-IDF Total", 0)),
                 ("Policy 2 TF-IDF Total", self.current_article.get("Policy 2 TF-IDF Total", 0)),
                 ("Policy 3 TF-IDF Total", self.current_article.get("Policy 3 TF-IDF Total", 0))],
                key=lambda x: x[1]
            )
            yield (self.current_file_name, max_policy), (self.current_article["ID"], max_value)
            del self.current_article

    def reduce_group_by_policy(self, key, articles):
        file_name, policy = key
        for article in articles:
            yield (file_name, policy), article

    def reduce_select_top3(self, key, articles):
        file_name, policy = key
        sorted_articles = sorted(articles, key=lambda x: x[1], reverse=True)[:3]

        # Ensure files are saved in the current working directory
        current_dir = os.getcwd()  # Get current directory where script runs
        output_dir = os.path.join(current_dir, "output_results")
        os.makedirs(output_dir, exist_ok=True)  # Create output directory if it doesn't exist
        output_file = os.path.join(output_dir, f"{file_name}_{policy.replace(' ', '_')}_top3.json")

        # Save JSON file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "file": file_name,
                    "policy": policy,
                    "top_3_articles": [
                        {"ID": article_id, "TF-IDF": tfidf}
                        for article_id, tfidf in sorted_articles
                    ],
                },
                f,
                ensure_ascii=False,
                indent=4,
            )

        yield (file_name, policy), f"File saved in: {output_file}"


if __name__ == '__main__':
    TopArticlesByPolicy.run()
