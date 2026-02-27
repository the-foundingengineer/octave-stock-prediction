from sqlalchemy import extract
from sqlalchemy.orm import Session
from app.models import IncomeStatement, Stock


def get_available_years(db: Session, stock_id: int):
    years = (
        db.query(extract("year", IncomeStatement.period_ending))
        .filter(IncomeStatement.stock_id == stock_id)
        .order_by(extract("year", IncomeStatement.period_ending).asc())
        .all()
    )

    return [int(y[0]) for y in years]

def get_metric_values(
    db: Session,
    stock_id: int,
    metric: str,
    years: list[int],
):
    results = (
        db.query(IncomeStatement)
        .filter(
            IncomeStatement.stock_id == stock_id,
            extract("year", IncomeStatement.period_ending).in_(years),
        )
        .all()
    )

    data = {}

    for row in results:
        year = row.period_ending.year
        value = getattr(row, metric, None)

        # Replace null with 0
        data[year] = value if value is not None else 0

    return data

def validate_nigerian_stock_question(question: str, db: Session) -> bool:

    question_lower = question.lower()

    # Fetch all Nigerian stock names
    nigerian_stocks = (
        db.query(Stock)
        .filter(Stock.country.ilike("nigeria"))
        .all()
    )

    # Check if any Nigerian stock name appears in question
    for stock in nigerian_stocks:
        if stock.name.lower() in question_lower:
            return True

    return False