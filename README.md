# IDX Exchange — Data Analyst Internship

Python-based MLS analytics pipeline using CRMLS data. The project progresses from raw data extraction through cleaning, feature engineering, and Tableau dashboard development over 12 weeks.

---

## Project Structure

```
IDXexchange/
├── raw/                          # Raw monthly CSV files from CoreLogic Trestle API
│   ├── CRMLSListing202401.csv
│   ├── CRMLSSold202401.csv
│   └── ...                       # Jan 2024 – Mar 2026
│
├── week1/                        # Dataset aggregation
│   ├── crmls_sold.py             # API extraction script — sold transactions
│   ├── crmls_listed.py           # API extraction script — listings
│   ├── sold_analysis.py          # ETL pipeline for sold transactions
│   ├── listed_analysis.py        # ETL pipeline for listed properties
│   ├── check_combined.py         # Post-aggregation validation
│   ├── combined_sold.csv         # All sold records, Jan 2024 – Mar 2026
│   └── combined_listed.csv       # All listing records, Jan 2024 – Mar 2026
│
├── week2/                        # EDA, validation & mortgage enrichment
│   ├── week2_eda_mortgage.py     # Main analysis script
│   ├── generate_report.py        # HTML report generator
│   ├── week2_report.html         # Self-contained deliverable report
│   ├── sold_residential_eda.csv
│   ├── listed_residential_eda.csv
│   ├── sold_null_summary.csv
│   ├── listed_null_summary.csv
│   ├── sold_numeric_distribution.csv
│   ├── mortgage_monthly_avg.csv
│   ├── sold_with_rates.csv       # Sold + FRED 30-yr mortgage rate
│   ├── listed_with_rates.csv     # Listed + FRED 30-yr mortgage rate
│   └── plots/                    # Distribution plots for 9 numeric fields
│
└── README.md
```

---

## Data Pipeline

```
CoreLogic Trestle API → Monthly CSVs → Aggregation → EDA & Cleaning → Feature Engineering → Tableau
```

**Two core datasets:**

| Dataset | Description |
|---|---|
| **Listings** | Market supply — all properties that entered the market, including active and failed-to-sell |
| **Sold** | Market outcomes — properties that successfully closed escrow |

Both datasets cover **January 2024 – March 2026**, filtered to **Residential** property type only.

---

## Weekly Progress

### Week 0 — MLS Data Pipeline Orientation
- Reviewed CoreLogic Trestle API authentication and pagination
- Ran `crmls_sold.py` and `crmls_listed.py` to extract monthly CSV files

### Week 1 — Dataset Aggregation
- Concatenated all monthly files (Jan 2024 – Mar 2026) into two combined datasets
- Filtered both to `PropertyType == 'Residential'`
- Validated row counts, date ranges, and duplicate records
- **Output:** `combined_sold.csv` (397,603 rows), `combined_listed.csv` (540,183 rows)

### Weeks 2–3 — Dataset Structuring, Validation & Mortgage Enrichment
- Documented unique property types and filtering logic
- Full missing value analysis — flagged 17 columns (sold) and 13 columns (listed) with >90% null
- Numeric distribution review for 9 key fields: histograms, boxplots, percentile summaries
- Answered core EDA questions: median close price ($820K), DOM distribution, above/below list price split, date consistency errors
- Fetched FRED `MORTGAGE30US` weekly series, resampled to monthly averages, merged onto both datasets
- **Output:** `sold_with_rates.csv`, `listed_with_rates.csv`, `week2_report.html`

### Weeks 4–5 — Data Cleaning *(upcoming)*
- Date field conversion and consistency flags
- Invalid numeric value removal
- Geographic data quality checks

### Week 6 — Feature Engineering *(upcoming)*
- Price ratio, price per sq ft, days-to-contract, contract-to-close

### Weeks 7–12 — Outlier Detection, Tableau Dashboards, Market Report *(upcoming)*

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Python / Pandas** | Data extraction, cleaning, feature engineering |
| **FRED API** | 30-year fixed mortgage rate enrichment |
| **Matplotlib** | Distribution plots and EDA visualizations |
| **Tableau Public** | Market and competitive analysis dashboards |
| **GitHub** | Version control and collaboration |

---

## Final Deliverables

- `sold_final.csv` — clean, analysis-ready sold transaction dataset
- `listed_final.csv` — clean, analysis-ready listings dataset
- Tableau dashboards published to Tableau Public

---

## Author

**Ruoyu Wang** — Data Analyst Intern @ IDX Exchange
