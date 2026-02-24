# from app.ai.gemini_service import call_gemini
# from sqlalchemy.orm import Session

# from app.ai.year_resolver import resolve_years
# from requests import Session
# from app.ai.year_resolver import resolve_years

# from app.ai.classifier import IntentType, classify_intent
# from app.ai.metric_mapper import extract_metric
# from app.ai.data_service import get_available_years, get_metric_values
# from app.ai.calculator import calculate_growth, forecast_next

# def handle_prediction(question: str, stock_id: int, db: Session):
#     intent = classify_intent(question)
#     metric = extract_metric(question)

#     available_years = get_available_years(db, stock_id)
#     resolved_years = resolve_years(intent, [], available_years)

#     if intent == IntentType.PREDICTION and len(resolved_years) < 2:
#         # Build AI-friendly explanation prompt
#         ai_prompt = (
#             f"User asked: '{question}'. "
#             "Sorry, there is not enough historical data to generate a reliable forecast."
#         )
#         ai_response = call_gemini(ai_prompt)  # function to send prompt to LLM
#         return {"answer": ai_response, "data_used": {}, "forecast": None}

#     # Continue normal prediction flow
#     values = get_metric_values(db, stock_id, metric, resolved_years)

#     forecast = None
#     growth = None

#     if intent == IntentType.PREDICTION:
#         growth = calculate_growth(values[resolved_years[0]], values[resolved_years[1]])
#         forecast = forecast_next(values[resolved_years[1]], growth)

#     if intent == IntentType.PREDICTION:
#         ai_prompt = (
#             f"User asked: '{question}'. "
#             f"{metric} in {resolved_years[0]}: {values[resolved_years[0]]}, "
#             f"in {resolved_years[1]}: {values[resolved_years[1]]}. "
#             f"Growth rate: {growth:.2%}. "
#             f"Forecast for next year: {forecast}. "
#             "Explain this trend and include a disclaimer."
#         )
#     else:
#         ai_prompt = (
#             f"User asked: '{question}'. "
#             f"Provide a concise explanation based on the following data: {values}."
#         )
#     ai_response = call_gemini(ai_prompt)

#     return {
#         "answer": ai_response,
#         "data_used": values,
#         "growth_rate": growth,
#         "forecast": forecast,
#         "disclaimer": "This forecast is based only on historical financial data and should not be considered investment advice."
#     }


# app/ai/service.py

from app.ai.classifier import classify_intent, IntentType
from app.ai.metric_mapper import extract_metric
from app.ai.year_resolver import resolve_years
from app.ai.data_service import get_available_years, get_metric_values
from app.ai.calculator import calculate_growth, forecast_next
from app.ai.gemini_service import call_gemini


def handle_prediction(question: str, stock_id: int, db):

    # Classify intent
    intent = classify_intent(question)

    # Extract metric
    metric = extract_metric(question)

    if metric is None:
        answer = call_gemini(
            f"User asked: '{question}'. "
            "The requested financial metric is not available in the database."
        )
        return {
            "answer": answer,
            "data_used": {},
            "growth_rate": None,
            "forecast": None,
            "disclaimer": None
        }

    # Get available years
    available_years = get_available_years(db, stock_id)

    if not available_years:
        raise ValueError("No financial data available for this stock.")

    # Resolve years (MVP: use latest years automatically)
    resolved_years = resolve_years(intent, [], available_years)

    # Ensure years are sorted oldest → newest
    resolved_years = sorted(resolved_years)

    # Fetch metric values
    values = get_metric_values(db, stock_id, metric, resolved_years)

    # Normalize year keys to int (prevents mismatch bug)
    values = {int(k): v for k, v in values.items()}

    growth = None
    forecast = None

    # Prediction Logic (SAFE VERSION)
    if intent == IntentType.PREDICTION:

        # Not enough data → let Gemini explain
        if len(resolved_years) < 2:
            answer = call_gemini(
                f"User asked: '{question}'. "
                "There is not enough historical data to generate a forecast."
            )
            return {
                "answer": answer,
                "data_used": values,
                "growth_rate": None,
                "forecast": None,
                "disclaimer": None
            }

        older_year = resolved_years[0]
        newer_year = resolved_years[1]

        older_value = values.get(older_year)
        newer_value = values.get(newer_year)

        if older_value is None or newer_value is None:
            raise ValueError("Missing financial data for required years.")

        # Growth with floor at zero
        growth = calculate_growth(older_value, newer_value)

        # Forecast next year
        forecast = forecast_next(newer_value, growth)

        ai_prompt = (
            f"User asked: '{question}'. "
            f"{metric} in {older_year}: {older_value}. "
            f"{metric} in {newer_year}: {newer_value}. "
            f"Growth rate: {growth:.2%}. "
            f"Forecast for next year: {forecast}. "
            "Explain the financial trend clearly and include a disclaimer."
        )

    else:
        # Non-prediction intents
        ai_prompt = (
            f"User asked: '{question}'. "
            f"Provide explanation based strictly on this data: {values}."
        )

    # Call Gemini
    answer = call_gemini(ai_prompt)

    # Structured response
    return {
        "answer": answer,
        "data_used": values,
        "growth_rate": growth,
        "forecast": forecast,
        "disclaimer": (
            "This forecast is based solely on historical financial data "
            "and should not be considered investment advice."
            if forecast is not None else None
        )
    }