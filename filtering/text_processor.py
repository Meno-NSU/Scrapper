import re
from nltk import wordpunct_tokenize
from nltk.stem.snowball import SnowballStemmer
from filtering.knowledge_cfg import KnowledgeConfig


class TextProcessor:
    def __init__(self, config: KnowledgeConfig):
        self.config = config
        self.stemmer = SnowballStemmer("russian")
        self.emoji_pattern = re.compile(
            r"[\U0001f600-\U0001f64f\U0001f300-\U0001f5ff\U0001f680-\U0001f6ff"
            r"\U0001f1e0-\U0001f1ff\U00002500-\U00002bef\U00002702-\U000027b0"
            r"\U000024c2-\U0001f251\U0001f926-\U0001f937\U00010000-\U0010ffff"
            r"\u2640-\u2642\u2600-\u2b55\u200d\u23cf\u23e9\u231a\ufe0f\u3030]+",
            re.UNICODE,
        )

    def _remove_emojis_from_string(self, text: str) -> str:
        return re.sub(self.emoji_pattern, "", text)

    def is_good_text(self, text: str) -> bool:
        normalized = " ".join(text.strip().lower().split()).strip()
        if len(normalized) == 0:
            return False
        return not any(
            bad_phrase in normalized for bad_phrase in self.config.bad_phrases
        )

    def clean_for_reading(self, text: str) -> str:
        text_no_emoji = self._remove_emojis_from_string(text)
        return " ".join(text_no_emoji.strip().split())

    def normalize_text(self, s: str) -> str:
        clean_s = self._remove_emojis_from_string(s)
        tokens = [t.strip() for t in wordpunct_tokenize(clean_s.strip().lower())]
        filtered_tokens = [t for t in tokens if len(t) > 2 and t.isalpha()]
        stemmed_words = [self.stemmer.stem(t) for t in filtered_tokens]
        words = [w for w in stemmed_words if len(w) > 0]
        return " ".join(words) if words else ""
