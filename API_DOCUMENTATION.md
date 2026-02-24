# Octave Stock API – Frontend Documentation

> **Base URL:** `http://localhost:8000`
>
> All endpoints return JSON. All `stock_id` parameters are integers.

---

## Quick Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/stocks` | Paginated stock list |
| `GET` | `/stocks/search` | Search by symbol/name |
| `GET` | `/stocks/compare` | Bulk comparison (multi-stock) |
| `GET` | `/stocks/{stock_id}` | Single stock detail |
| `GET` | `/stocks/{stock_id}/klines` | OHLCV chart data |
| `GET` | `/stocks/{stock_id}/stats` | Key statistics |
| `GET` | `/stocks/{stock_id}/info` | Profile + technicals |
| `GET` | `/stocks/{stock_id}/related` | Same-sector stocks |
| `GET` | `/stocks/{stock_id}/dividends` | Dividend history |
| `GET` | `/stocks/{stock_id}/market-cap` | Market cap history |
| `GET` | `/stocks/{stock_id}/financials/income-statement` | Income statement |
| `GET` | `/stocks/{stock_id}/comparison` | Full comparison data |
| `GET` | `/stocks/{stock_id}/profile` | Company profile + executives |
| `GET` | `/stocks/{stock_id}/executives` | Management team |
| `GET` | `/popular_comparisons` | Top stocks per sector |
| `POST` | `/stock_records/` | Create a daily kline record |
| `POST` | `/stocks/{symbol}/refresh` | Refresh from iTick API |

---

## Endpoints

### 1. List Stocks

```
GET /stocks?page={page}&limit={limit}
```

**Query Parameters:**

| Param | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `page` | `int` | `1` | `>= 1` | Page number |
| `limit` | `int` | `10` | `1–100` | Items per page |

**Response:** `Stock[]`

```json
[
  {
    "id": 1,
    "symbol": "DANGCEM",
    "name": "Dangote Cement Plc",
    "sector": "Industrial Goods",
    "industry": "Building Materials",
    "description": "...",
    "website": "https://dangotecement.com",
    "currency": "NGN",
    "exchange": "NGX",
    "last_updated": "2025-02-18T12:00:00"
  }
]
```

---

### 2. Search Stocks

```
GET /stocks/search?q={query}&limit={limit}
```

**Query Parameters:**

| Param | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `q` | `string` | *required* | min 1 char | Search query (symbol or name) |
| `limit` | `int` | `10` | `1–50` | Max results |

**Response:** `StockSearchResult[]`

```json
[
  {
    "id": 1,
    "symbol": "DANGCEM",
    "name": "Dangote Cement Plc",
    "sector": "Industrial Goods"
  }
]
```

> **Frontend tip:** Use this for autocomplete/typeahead search bars. Debounce requests by 300ms.

---

### 3. Single Stock Detail

```
GET /stocks/{stock_id}
```

**Response:** `Stock` (same shape as list items)

```json
{
  "id": 1,
  "symbol": "DANGCEM",
  "name": "Dangote Cement Plc",
  "sector": "Industrial Goods",
  "industry": "Building Materials",
  "description": "Dangote Cement Plc is a cement manufacturer...",
  "website": "https://dangotecement.com",
  "currency": "NGN",
  "exchange": "NGX",
  "last_updated": "2025-02-18T12:00:00"
}
```

**Errors:** `404` if stock_id not found.

---

### 4. Klines (Chart Data)

```
GET /stocks/{stock_id}/klines?interval={interval}&limit={limit}
```

**Query Parameters:**

| Param | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `interval` | `string` | *required* | `day`, `week`, `month`, `year` | Candle interval |
| `limit` | `int` | `500` | `1–1000` | Max candles to return |

Also accepted: `1d`, `daily`, `1w`, `weekly`, `1m`, `monthly`, `1y`, `yearly`

**Response:** `KlineResponse`

```json
{
  "stock_id": 1,
  "symbol": "DANGCEM",
  "interval": "week",
  "klines": [
    {
      "date": "2025-02-14",
      "open": 450.0,
      "high": 465.0,
      "low": 445.0,
      "close": 460.0,
      "volume": 1250000.0
    }
  ]
}
```

> **Frontend tip:** Use `day` for intraday charts, `week` for the default chart view, `month`/`year` for long-term analysis. Klines are returned newest-first for non-daily intervals.

---

### 5. Stock Statistics

```
GET /stocks/{stock_id}/stats
```

**Response:** `StockStatsResponse`

```json
{
  "stock_id": 1,
  "symbol": "DANGCEM",
  "market_cap": 5123456789000.0,
  "revenue_ttm": 1250000000000.0,
  "net_income": 350000000000.0,
  "eps": 19.44,
  "shares_outstanding": 17040507405,
  "pe_ratio": 22.5,
  "forward_pe": null,
  "dividend": 30.0,
  "ex_dividend_date": "2024-06-10",
  "volume": 2500000,
  "avg_volume": 3200000,
  "open": 455.0,
  "previous_close": 450.0,
  "day_range": "445.00 - 465.00",
  "fifty_two_week_range": "380.00 - 520.00",
  "beta": 0.85,
  "rsi": 55.3,
  "earnings_date": null,
  "payout_ratio": 65.2,
  "dividend_growth": null,
  "payout_frequency": "Annual",
  "revenue_growth": 12.5,
  "revenue_per_employee": null
}
```

> **Frontend tip:** All fields except `stock_id` and `symbol` can be `null`. Always handle nulls gracefully in the UI (e.g., display "N/A" or "--").

---

### 6. Stock Info (Technicals)

```
GET /stocks/{stock_id}/info
```

**Response:** `StockInfoResponse`

```json
{
  "stock_id": 1,
  "symbol": "DANGCEM",
  "ipo_date": null,
  "name": "Dangote Cement Plc",
  "fifty_two_week_high": 520.0,
  "fifty_two_week_low": 380.0,
  "fifty_day_moving_average": 448.5,
  "sector": "Industrial Goods",
  "industry": "Building Materials",
  "sentiment": null,
  "sp_score": null
}
```

---

### 7. Related Stocks

```
GET /stocks/{stock_id}/related?limit={limit}
```

**Query Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `limit` | `int` | `10` | Max related stocks to return |

**Response:** `StockRelatedResponse[]`

```json
[
  {
    "stock_id": 5,
    "symbol": "BUACEMENT",
    "market_cap": 2500000000000.0,
    "revenue_ttm": 850000000000.0
  }
]
```

> **Frontend tip:** Use for "Similar Stocks" or "You might also like" sections. Returns stocks in the same sector.

---

### 8. Dividends

```
GET /stocks/{stock_id}/dividends
```

**Response:** `DividendResponse[]`

```json
[
  {
    "id": 42,
    "stock_id": 1,
    "ex_dividend_date": "2024-06-10",
    "record_date": "2024-06-12",
    "pay_date": "2024-07-15",
    "amount": 30.0,
    "currency": "NGN",
    "frequency": "Annual"
  }
]
```

Results are ordered by `ex_dividend_date` descending (newest first).

---

### 9. Market Cap History

```
GET /stocks/{stock_id}/market-cap?limit={limit}
```

**Query Parameters:**

| Param | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `limit` | `int` | `500` | `1–5000` | Max records |

**Response:** `MarketCapHistoryResponse`

```json
{
  "stock_id": 1,
  "symbol": "DANGCEM",
  "history": [
    {
      "id": 100,
      "stock_id": 1,
      "date": "2025-02-18",
      "market_cap": 5123456789000.0,
      "frequency": "daily"
    }
  ]
}
```

---

### 10. Income Statement

```
GET /stocks/{stock_id}/financials/income-statement
```

**Response:** `StockWithIncomeStatementResponse`

```json
{
  "id": 1,
  "symbol": "DANGCEM",
  "name": "Dangote Cement Plc",
  "sector": "Industrial Goods",
  "industry": "Building Materials",
  "exchange": "NGX",
  "currency": "NGN",
  "country": "Nigeria",
  "website": "https://dangotecement.com",
  "ceo": "Michel Puchercos",
  "employees": 15000,
  "fiscal_year_end": "December",
  "headquarters": "Lagos, Nigeria",
  "income_statement": {
    "id": 10,
    "stock_id": 1,
    "period_ending": "2024-12-31",
    "period_type": "FY",
    "revenue": 1250000000000.0,
    "operating_revenue": null,
    "other_revenue": null,
    "revenue_growth_yoy": 0.125,
    "cost_of_revenue": 650000000000.0,
    "gross_profit": 600000000000.0,
    "sga_expenses": null,
    "operating_income": 450000000000.0,
    "ebitda": 520000000000.0,
    "ebit": 440000000000.0,
    "interest_expense": 25000000000.0,
    "pretax_income": 420000000000.0,
    "income_tax": 70000000000.0,
    "net_income": 350000000000.0,
    "net_income_growth_yoy": 0.15,
    "eps_basic": 19.44,
    "eps_diluted": 19.40,
    "eps_growth_yoy": 0.12,
    "dividend_per_share": 30.0,
    "shares_basic": 17040507405,
    "shares_diluted": 17040507405
  }
}
```

> **Frontend tip:** `income_statement` can be `null` if no financial data is available.

---

### 11. Stock Comparison (Single)

```
GET /stocks/{stock_id}/comparison
```

**Response:** `StockComparisonItem` — a comprehensive object with **80+ fields** covering:

| Section | Fields |
|---------|--------|
| **Basic info** | `symbol`, `name`, `sector`, `industry`, `exchange`, `website`, `country`, `employees`, `founded`, `ipo_date` |
| **Price** | `stock_price`, `price_change_1d`, `price_change_percent_1d`, `open_price`, `previous_close`, `low_price`, `high_price`, `volume`, `dollar_volume`, `stock_price_date` |
| **52-week** | `fifty_two_week_low`, `fifty_two_week_high` |
| **Valuation** | `market_cap`, `enterprise_value`, `pe_ratio`, `forward_pe`, `ps_ratio`, `pb_ratio`, `peg_ratio`, `ev_sales`, `ev_ebitda`, `ev_ebit`, `ev_fcf`, `earnings_yield`, `fcf_yield` |
| **Financials** | `revenue`, `gross_profit`, `operating_income`, `net_income`, `ebitda`, `ebit`, `eps`, `revenue_growth`, `net_income_growth`, `eps_growth` |
| **Margins** | `gross_margin`, `operating_margin`, `profit_margin`, `fcf_margin` |
| **Cash flow** | `operating_cash_flow`, `investing_cash_flow`, `financing_cash_flow`, `net_cash_flow`, `capital_expenditures`, `free_cash_flow` |
| **Balance sheet** | `total_cash`, `total_debt`, `net_cash_debt`, `total_assets`, `total_liabilities`, `shareholders_equity`, `working_capital`, `book_value_per_share`, `shares_outstanding` |
| **Ratios** | `roe`, `roa`, `roic`, `roce`, `current_ratio`, `quick_ratio`, `debt_equity`, `debt_ebitda`, `interest_coverage`, `altman_z_score`, `piotroski_f_score` |
| **Technicals** | `rsi`, `beta`, `ma_20`, `ma_50`, `ma_200` |
| **Dividends** | `dividend_yield`, `dividend_per_share`, `ex_div_date`, `payout_ratio`, `dividend_growth`, `payout_frequency`, `revenue_ttm`, `revenue_growth`, `revenue_per_employee` |

> **Frontend tip:** Used for side-by-side comparison pages. All fields are nullable.

---

### 12. Bulk Compare (Multi-Stock)

```
GET /stocks/compare?symbols={symbols}&interval={interval}&limit={limit}
```

**Query Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `symbols` | `string` | *required* | Comma-separated symbols (e.g., `DANGCEM,BUACEMENT,MTNN`) |
| `interval` | `string` | `week` | Kline interval: `day`, `week`, `month`, `year` |
| `limit` | `int` | `52` | Max klines per stock (1–500) |

**Response:** `BulkComparisonResponse`

```json
{
  "comparisons": [
    {
      "stock_id": 1,
      "symbol": "DANGCEM",
      "klines": [
        { "date": "2025-02-14", "open": 450.0, "high": 465.0, "low": 445.0, "close": 460.0, "volume": 1250000.0 }
      ],
      "stats": {
        "stock_id": 1,
        "symbol": "DANGCEM",
        "market_cap": 5123456789000.0,
        "pe_ratio": 22.5
      }
    }
  ]
}
```

> **Frontend tip:** Use this for multi-stock chart overlays. Each item has both `klines` (for the chart) and `stats` (for the comparison table). `stats` follows the same shape as the `/stats` endpoint.

---

### 13. Popular Comparisons

```
GET /popular_comparisons
```

**Response:** `PopularComparisonResponse`

```json
{
  "stocks": [
    { "id": 1, "symbol": "DANGCEM", "sector": "Industrial Goods", "rank": 1 },
    { "id": 5, "symbol": "BUACEMENT", "sector": "Industrial Goods", "rank": 2 },
    { "id": 10, "symbol": "MTNN", "sector": "ICT", "rank": 1 },
    { "id": 15, "symbol": "AIRTELAFRI", "sector": "ICT", "rank": 2 }
  ]
}
```

Returns the **top 2 stocks per sector** ranked by market cap. Use as suggestions for the comparison feature.

---

### 14. Company Profile

```
GET /stocks/{stock_id}/profile
```

**Response:** `StockProfileResponse`

```json
{
  "id": 1,
  "symbol": "DANGCEM",
  "name": "Dangote Cement Plc",
  "description": "Dangote Cement is a leading cement manufacturer...",
  "sector": "Industrial Goods",
  "industry": "Building Materials",
  "exchange": "NGX",
  "currency": "NGN",
  "country": "Nigeria",
  "founded": "1992",
  "headquarters": "Lagos, Nigeria",
  "website": "https://dangotecement.com",
  "employees": 15000,
  "ceo": "Michel Puchercos",
  "executives": [
    { "id": 1, "name": "Michel Puchercos", "title": "CEO", "age": 58, "since": "2020" },
    { "id": 2, "name": "Guillaume Moyet", "title": "CFO", "age": null, "since": null }
  ]
}
```

---

### 15. Executives

```
GET /stocks/{stock_id}/executives
```

**Response:** `StockExecutiveResponse[]`

```json
[
  { "id": 1, "name": "Michel Puchercos", "title": "CEO", "age": 58, "since": "2020" },
  { "id": 2, "name": "Guillaume Moyet", "title": "CFO", "age": null, "since": null }
]
```

---

### 16. Create Stock Record

```
POST /stock_records/
```

**Request Body:**

```json
{
  "date": "2025-02-18",
  "open": 450.0,
  "high": 465.0,
  "low": 445.0,
  "close": 460.0,
  "volume": 1250000,
  "symbol": "DANGCEM"
}
```

**Response:** `StockRecord` (same fields + `id`)

---

### 17. Refresh Stock Data

```
POST /stocks/{symbol}/refresh?token={api_token}
```

**Path Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `symbol` | `string` | Stock symbol (e.g., `DANGCEM`) |

**Query Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `token` | `string` | iTick API token |

**Response:** Updated stock object.

**Errors:** `400` if API call fails.

---

## Error Handling

All endpoints return errors in this format:

```json
{
  "detail": "Stock not found"
}
```

| HTTP Code | Meaning |
|-----------|---------|
| `400` | Bad request (invalid params) |
| `404` | Resource not found |
| `422` | Validation error (missing/invalid query params) |
| `500` | Server error |

> **Frontend tip:** Always check for HTTP status codes. For `422` errors, the response body includes field-level validation details in `detail[].loc` and `detail[].msg`.

---

## TypeScript Interfaces

Here are TypeScript types you can use directly in your frontend:

```typescript
// ── Core Types ─────────────────────────────────────

interface Stock {
  id: number;
  symbol: string;
  name: string | null;
  sector: string | null;
  industry: string | null;
  description: string | null;
  website: string | null;
  currency: string | null;
  exchange: string | null;
  last_updated: string | null;
}

interface StockSearchResult {
  id: number;
  symbol: string;
  name: string | null;
  sector: string | null;
}

// ── Klines ─────────────────────────────────────────

interface KlineData {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface KlineResponse {
  stock_id: number;
  symbol: string;
  interval: string;
  klines: KlineData[];
}

// ── Stats ──────────────────────────────────────────

interface StockStats {
  stock_id: number;
  symbol: string;
  market_cap: number | null;
  revenue_ttm: number | null;
  net_income: number | null;
  eps: number | null;
  shares_outstanding: number | null;
  pe_ratio: number | null;
  forward_pe: number | null;
  dividend: number | null;
  ex_dividend_date: string | null;
  volume: number | null;
  avg_volume: number | null;
  open: number | null;
  previous_close: number | null;
  day_range: string | null;
  fifty_two_week_range: string | null;
  beta: number | null;
  rsi: number | null;
  earnings_date: string | null;
  payout_ratio: number | null;
  dividend_growth: number | null;
  payout_frequency: string | null;
  revenue_growth: number | null;
  revenue_per_employee: number | null;
}

// ── Info ───────────────────────────────────────────

interface StockInfo {
  stock_id: number;
  symbol: string;
  ipo_date: string | null;
  name: string | null;
  fifty_two_week_high: number | null;
  fifty_two_week_low: number | null;
  fifty_day_moving_average: number | null;
  sector: string | null;
  industry: string | null;
  sentiment: string | null;
  sp_score: number | null;
}

// ── Related ────────────────────────────────────────

interface StockRelated {
  stock_id: number;
  symbol: string;
  market_cap: number | null;
  revenue_ttm: number | null;
}

// ── Dividends ──────────────────────────────────────

interface DividendData {
  id: number;
  stock_id: number;
  ex_dividend_date: string;
  record_date: string | null;
  pay_date: string | null;
  amount: number;
  currency: string | null;
  frequency: string | null;
}

// ── Market Cap History ─────────────────────────────

interface MarketCapHistoryItem {
  id: number;
  stock_id: number;
  date: string;
  market_cap: number | null;
  frequency: string | null;
}

interface MarketCapHistoryResponse {
  stock_id: number;
  symbol: string;
  history: MarketCapHistoryItem[];
}

// ── Income Statement ───────────────────────────────

interface IncomeStatement {
  id: number;
  stock_id: number;
  period_ending: string;
  period_type: string;
  revenue: number | null;
  operating_revenue: number | null;
  other_revenue: number | null;
  revenue_growth_yoy: number | null;
  cost_of_revenue: number | null;
  gross_profit: number | null;
  sga_expenses: number | null;
  operating_income: number | null;
  ebitda: number | null;
  ebit: number | null;
  interest_expense: number | null;
  pretax_income: number | null;
  income_tax: number | null;
  net_income: number | null;
  net_income_growth_yoy: number | null;
  eps_basic: number | null;
  eps_diluted: number | null;
  eps_growth_yoy: number | null;
  dividend_per_share: number | null;
  shares_basic: number | null;
  shares_diluted: number | null;
}

interface StockWithIncomeStatement {
  id: number;
  symbol: string;
  name: string | null;
  sector: string | null;
  industry: string | null;
  exchange: string | null;
  currency: string | null;
  country: string | null;
  website: string | null;
  ceo: string | null;
  employees: number | null;
  fiscal_year_end: string | null;
  headquarters: string | null;
  income_statement: IncomeStatement | null;
}

// ── Comparisons ────────────────────────────────────

interface StockComparisonBrief {
  id: number;
  symbol: string;
  sector: string | null;
  rank: number | null;
}

interface PopularComparisonResponse {
  stocks: StockComparisonBrief[];
}

interface BulkComparisonItem {
  stock_id: number;
  symbol: string;
  klines: KlineData[];
  stats: StockStats | null;
}

interface BulkComparisonResponse {
  comparisons: BulkComparisonItem[];
}

// ── Profile & Executives ───────────────────────────

interface StockExecutive {
  id: number;
  name: string;
  title: string | null;
  age: number | null;
  since: string | null;
}

interface StockProfile {
  id: number;
  symbol: string;
  name: string | null;
  description: string | null;
  sector: string | null;
  industry: string | null;
  exchange: string | null;
  currency: string | null;
  country: string | null;
  founded: string | null;
  headquarters: string | null;
  website: string | null;
  employees: number | null;
  ceo: string | null;
  executives: StockExecutive[];
}
```
