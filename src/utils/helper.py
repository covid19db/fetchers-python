from typing import List


def remove_words(data: str, words: List) -> str:
    for w in words:
        data = data.replace(w, '')
    return data.strip()
