METRIC_ALIASES = {
    "revenue": ["revenue", "total revenue", "sales"],
    "operating_revenue": ["operating revenue"],
    "gross_profit": ["gross profit"],
    "net_income": ["net income", "profit", "earnings"],
    "operating_income": ["operating income"],
    "ebit": ["ebit"],
    "ebitda": ["ebitda"],
    "pretax_income": ["pretax income", "income before tax"],
    "eps_basic": ["eps", "earnings per share", "basic eps"],
    "eps_diluted": ["diluted eps"],
    "dividend_per_share": ["dividend", "dividend per share"],
}

def extract_metric(question: str) -> str | None:
    q = question.lower()

    for db_column, aliases in METRIC_ALIASES.items():
        for alias in aliases:
            if alias in q:
                return db_column

    return None