from typing import List


def remove_words(query: str, words: List) -> str:
    return (' '.join(filter(lambda x: x not in words, query.split()))).strip()


