import re
from typing import List


def extract_years(question: str) -> List[int]:
    years = re.findall(r"\b(20\d{2})\b", question)
    return [int(y) for y in years]


def resolve_years(intent, extracted_years, available_years):
    """
    intent: IntentType
    extracted_years: list[int]
    available_years: list[int] sorted ascending
    """

    if intent == "prediction":
        # Always use latest two
        return available_years[-2:]

    if intent in ["comparison", "growth"]:
        if len(extracted_years) >= 2:
            return extracted_years[:2]
        else:
            # fallback: use latest two
            return available_years[-2:]

    if intent == "lookup":
        if extracted_years:
            return [extracted_years[0]]
        else:
            return [available_years[-1]]  # latest

    if intent == "summary":
        return available_years  # use all available

    return [available_years[-1]]