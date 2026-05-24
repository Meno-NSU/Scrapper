from dataclasses import dataclass, field
from typing import List

@dataclass(frozen=True)
class KnowledgeConfig:
    min_num_words: int = 10
    max_f1: float = 0.9
    bad_phrases: List[str] = field(
        default_factory=lambda: [
            "402 please renew your subscription",
            "страница не найдена",
            "page not found",
        ]
    )