# IDX Exchange Data Analyst Internship

## Overview
This repository contains Python scripts and analysis work for the IDX Exchange Data Analyst Internship. The project focuses on real estate market analysis using CRMLS (California Regional Multiple Listing Service) data.

## Project Structure
├── week1/
│   ├── sold_analysis.py      # ETL pipeline for sold transactions
│   ├── listed_analysis.py    # ETL pipeline for listed properties
│   └── out/                  # Output CSV files (not tracked)
├── week2/
│   └── ...
└── README.md

## Data Pipeline
**API → Python ETL → Clean CSV Tables → Tableau Data Source → Dashboard**

### Two Datasets:
- **Listings (CSV 1):** Market supply - all properties that entered the market, including active and failed-to-sell
- **Sold (CSV 2):** Market outcomes - properties that successfully closed

## Week 1 Tasks
- Fetch and append all CSV files from January 2024 through March 2026
- Generate monthly data using `crmls_sold.py` and `crmls_listed.py`
- Perform exploratory data analysis in Python
- Begin feature engineering

## Tech Stack
- **Python:** Data extraction, cleaning, and feature engineering
- **Tableau:** Market and competitive analysis dashboards
- **GitHub:** Version control and code collaboration

## Output
Final deliverables are clean, structured CSV files ready for Tableau visualization:
- `sold_final.csv` - Sold transaction analysis
- `listed_final.csv` - Listed property analysis

## Tableau Public
Dashboards will be published to Tableau Public for market and competitive analysis visualization.

## Author
Ruoyu - Data Analyst Intern @ IDX Exchange# IDXExchange-Intern
