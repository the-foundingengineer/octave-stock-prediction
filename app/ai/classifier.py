from enum import Enum
import re
# from .classifier import In

class IntentType(str, Enum):
    LOOKUP = "lookup"
    COMPARISON = "comparison"
    GROWTH = "growth"
    SUMMARY = "summary"
    PREDICTION = "prediction"

PREDICTION_KEYWORDS = [
    "forecast",
    "predict",
    "projection",
    "next year",
    "future",
    "outlook"
]

GROWTH_KEYWORDS = ["growth", "increase rate"]
COMPARISON_KEYWORDS = ["compare", "difference", "vs"]
SUMMARY_KEYWORDS = ["summarize", "performance", "overview"]

def classify_intent(question: str) -> IntentType:
    q = question.lower()

    if any(keyword in q for keyword in PREDICTION_KEYWORDS):
        return IntentType.PREDICTION
    
    if any(keyword in q for keyword in GROWTH_KEYWORDS):
        return IntentType.PREDICTION
    
    if any(keyword in q for keyword in COMPARISON_KEYWORDS):
        return IntentType.PREDICTION
    
    if any(keyword in q for keyword in SUMMARY_KEYWORDS):
        return IntentType.PREDICTION
    
    return IntentType.LOOKUP