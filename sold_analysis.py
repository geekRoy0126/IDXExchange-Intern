"""
Sold Transactions Analysis Pipeline
Fetches CRMLS sold data from Jan 2024 through Mar 2026,
performs data cleaning and feature engineering,
and outputs a final CSV ready for Tableau dashboards.
"""

import csv
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ── Configuration ─────────────────────────────────────────────────────────────
AUTH_ENDPOINT = 'https://idxexchange.com/internal-api/trestle_token.php?key=IDXEXCHANGE2026_CHANGE_THIS'
API_URL       = 'https://api-trestle.corelogic.com/trestle/odata/Property'
OUTPUT_DIR    = r'D:\IDXexchange\out'
MONTHLY_DIR   = os.path.join(OUTPUT_DIR, 'monthly_sold')
FINAL_CSV     = os.path.join(OUTPUT_DIR, 'sold_final.csv')

START_MONTH   = datetime(2024, 1, 1)
END_MONTH     = datetime(2026, 3, 1)   # inclusive

FIELDS = [
    'BuyerAgentAOR','ListAgentAOR','Flooring','ViewYN','WaterfrontYN','BasementYN',
    'PoolPrivateYN','OriginalListPrice','ListingKey','ListAgentEmail','CloseDate',
    'ClosePrice','ListAgentFirstName','ListAgentLastName','Latitude','Longitude',
    'UnparsedAddress','PropertyType','LivingArea','ListPrice','DaysOnMarket',
    'ListOfficeName','BuyerOfficeName','CoListOfficeName','ListAgentFullName',
    'CoListAgentFirstName','CoListAgentLastName','BuyerAgentMlsId',
    'BuyerAgentFirstName','BuyerAgentLastName','FireplacesTotal',
    'AssociationFeeFrequency','AboveGradeFinishedArea','ListingKeyNumeric',
    'MLSAreaMajor','TaxAnnualAmount','CountyOrParish','MlsStatus',
    'ElementarySchool','AttachedGarageYN','ParkingTotal','BuilderName',
    'PropertySubType','LotSizeAcres','SubdivisionName','BuyerOfficeAOR',
    'YearBuilt','StreetNumberNumeric','ListingId','BathroomsTotalInteger',
    'City','TaxYear','BuildingAreaTotal','BedroomsTotal','ContractStatusChangeDate',
    'ElementarySchoolDistrict','CoBuyerAgentFirstName','PurchaseContractDate',
    'ListingContractDate','BelowGradeFinishedArea','BusinessType','StateOrProvince',
    'CoveredSpaces','MiddleOrJuniorSchool','FireplaceYN','Stories','HighSchool',
    'Levels','LotSizeDimensions','LotSizeArea','MainLevelBedrooms',
    'NewConstructionYN','GarageSpaces','HighSchoolDistrict','PostalCode',
    'AssociationFee','LotSizeSquareFeet','MiddleOrJuniorSchoolDistrict',
    'OriginatingSystemName','OriginatingSystemSubName',
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_token():
    r = requests.get(AUTH_ENDPOINT, timeout=30)
    r.raise_for_status()
    token = r.json().get('access_token')
    if not token:
        raise RuntimeError("Could not retrieve access token.")
    return token


def fetch_month(token, year, month):
    """Fetch all sold records for a given year/month, return list of dicts."""
    start = datetime(year, month, 1)
    end   = start + relativedelta(months=1)
    headers = {'Authorization': f'Bearer {token}'}
    params = {
        '$select': ','.join(FIELDS),
        '$filter': (
            f"MlsStatus eq 'Closed' "
            f"and CloseDate ge {start.isoformat(timespec='milliseconds')}Z "
            f"and CloseDate lt {end.isoformat(timespec='milliseconds')}Z"
        ),
        '$top': 1000,
    }
    url = API_URL
    records = []
    while True:
        r = requests.get(url, params=params, headers=headers, timeout=60)
        if r.status_code != 200:
            print(f"  API error {r.status_code}: {r.text[:200]}")
            break
        data = r.json()
        batch = data.get('value', [])
        records.extend(batch)
        if '@odata.nextLink' in data:
            url    = data['@odata.nextLink']
            params = None
        else:
            break
    return records


def save_monthly_csv(records, year, month):
    path = os.path.join(MONTHLY_DIR, f'CRMLSSold{year}{month:02d}.csv')
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)
    return path


def month_csv_path(year, month):
    return os.path.join(MONTHLY_DIR, f'CRMLSSold{year}{month:02d}.csv')

# ── Data collection ────────────────────────────────────────────────────────────

def collect_all_months():
    os.makedirs(MONTHLY_DIR, exist_ok=True)
    token = get_token()
    current = START_MONTH
    all_paths = []
    while current <= END_MONTH:
        y, m = current.year, current.month
        path = month_csv_path(y, m)
        if os.path.exists(path) and os.path.getsize(path) > 100:
            print(f"  [{y}-{m:02d}] Already cached: {path}")
        else:
            print(f"  [{y}-{m:02d}] Fetching from API...")
            records = fetch_month(token, y, m)
            save_monthly_csv(records, y, m)
            print(f"           {len(records)} records saved.")
        all_paths.append(path)
        current += relativedelta(months=1)
    return all_paths

# ── Data cleaning & feature engineering ──────────────────────────────────────

def clean_and_engineer(df):
    # ── Numeric coercion ──
    numeric_cols = [
        'ClosePrice','ListPrice','OriginalListPrice','LivingArea',
        'AboveGradeFinishedArea','BelowGradeFinishedArea','BuildingAreaTotal',
        'LotSizeSquareFeet','LotSizeAcres','LotSizeArea',
        'DaysOnMarket','BedroomsTotal','BathroomsTotalInteger','FireplacesTotal',
        'GarageSpaces','CoveredSpaces','ParkingTotal','Stories','MainLevelBedrooms',
        'YearBuilt','TaxYear','TaxAnnualAmount','AssociationFee',
        'Latitude','Longitude','StreetNumberNumeric','ListingKeyNumeric',
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # ── Date coercion ──
    date_cols = ['CloseDate','ListingContractDate','PurchaseContractDate','ContractStatusChangeDate']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_localize(None)

    # ── Boolean coercion ──
    bool_cols = ['ViewYN','WaterfrontYN','BasementYN','PoolPrivateYN','FireplaceYN',
                 'AttachedGarageYN','NewConstructionYN']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map(lambda x: True if str(x).strip().lower() in ('true','1','yes') else
                                             (False if str(x).strip().lower() in ('false','0','no') else np.nan))

    # ── Drop duplicates ──
    if 'ListingKey' in df.columns:
        df = df.drop_duplicates(subset='ListingKey', keep='last')

    # ── Drop rows missing core financial fields ──
    df = df.dropna(subset=['ClosePrice','CloseDate'])
    df = df[df['ClosePrice'] > 0]

    # ── Feature engineering ──
    # Price per sq ft
    df['price_per_sqft'] = np.where(
        (df['LivingArea'].notna()) & (df['LivingArea'] > 0),
        df['ClosePrice'] / df['LivingArea'], np.nan
    )

    # List-to-close price ratio
    df['list_to_close_ratio'] = np.where(
        (df['ListPrice'].notna()) & (df['ListPrice'] > 0),
        df['ClosePrice'] / df['ListPrice'], np.nan
    )

    # Original-list to close ratio (discount from original)
    df['orig_list_to_close_ratio'] = np.where(
        (df['OriginalListPrice'].notna()) & (df['OriginalListPrice'] > 0),
        df['ClosePrice'] / df['OriginalListPrice'], np.nan
    )

    # Days-on-market category
    df['dom_category'] = pd.cut(
        df['DaysOnMarket'],
        bins=[-1, 7, 30, 90, float('inf')],
        labels=['Quick (0-7d)', 'Normal (8-30d)', 'Slow (31-90d)', 'Very Slow (90d+)']
    )

    # Property age at close
    df['property_age'] = np.where(
        (df['YearBuilt'].notna()) & (df['CloseDate'].notna()),
        df['CloseDate'].dt.year - df['YearBuilt'], np.nan
    )

    # Close year / month
    df['close_year']  = df['CloseDate'].dt.year
    df['close_month'] = df['CloseDate'].dt.month
    df['close_year_month'] = df['CloseDate'].dt.to_period('M').astype(str)

    # HOA flag
    df['has_hoa'] = (df['AssociationFee'].notna()) & (df['AssociationFee'] > 0)

    # Feature flags (ensure bool columns exist)
    df['has_pool']       = df.get('PoolPrivateYN', False).fillna(False).astype(bool)
    df['has_view']       = df.get('ViewYN', False).fillna(False).astype(bool)
    df['has_waterfront'] = df.get('WaterfrontYN', False).fillna(False).astype(bool)
    df['has_basement']   = df.get('BasementYN', False).fillna(False).astype(bool)
    df['has_fireplace']  = df.get('FireplaceYN', False).fillna(False).astype(bool)
    df['has_garage']     = df.get('AttachedGarageYN', False).fillna(False).astype(bool)
    df['is_new_construction'] = df.get('NewConstructionYN', False).fillna(False).astype(bool)

    # Bed / bath ratio
    df['bed_bath_ratio'] = np.where(
        (df['BathroomsTotalInteger'].notna()) & (df['BathroomsTotalInteger'] > 0),
        df['BedroomsTotal'] / df['BathroomsTotalInteger'], np.nan
    )

    # Price buckets (based on close price)
    df['price_bucket'] = pd.cut(
        df['ClosePrice'],
        bins=[0, 300_000, 500_000, 750_000, 1_000_000, 1_500_000, float('inf')],
        labels=['<$300K','$300K-$500K','$500K-$750K','$750K-$1M','$1M-$1.5M','>$1.5M']
    )

    # Lot size sqft fallback from acres
    df['lot_sqft'] = np.where(
        df['LotSizeSquareFeet'].notna(),
        df['LotSizeSquareFeet'],
        np.where(df['LotSizeAcres'].notna(), df['LotSizeAcres'] * 43560, np.nan)
    )

    # Above + below grade total finished area
    df['total_finished_area'] = df[['AboveGradeFinishedArea','BelowGradeFinishedArea']].sum(axis=1, min_count=1)

    # Buyer same office as list office (in-house deal)
    df['in_house_deal'] = (
        df['ListOfficeName'].notna() &
        df['BuyerOfficeName'].notna() &
        (df['ListOfficeName'].str.strip() == df['BuyerOfficeName'].str.strip())
    )

    return df

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=== SOLD TRANSACTIONS PIPELINE ===")
    print(f"Date range: {START_MONTH.strftime('%Y-%m')} to {END_MONTH.strftime('%Y-%m')}")
    print()

    # Step 1: Collect monthly CSVs
    print("Step 1: Collecting monthly data...")
    paths = collect_all_months()
    print(f"  Total months collected: {len(paths)}")
    print()

    # Step 2: Load and append all CSVs
    print("Step 2: Loading and appending all monthly CSVs...")
    frames = []
    for path in paths:
        try:
            df_m = pd.read_csv(path, dtype=str, low_memory=False)
            frames.append(df_m)
        except Exception as e:
            print(f"  Warning: could not read {path}: {e}")
    df = pd.concat(frames, ignore_index=True)
    print(f"  Total rows before cleaning: {len(df):,}")
    print()

    # Step 3: Clean and feature engineer
    print("Step 3: Cleaning data and engineering features...")
    df = clean_and_engineer(df)
    print(f"  Total rows after cleaning:  {len(df):,}")
    print()

    # Step 4: Summary stats
    print("Step 4: Exploratory summary")
    print(f"  Unique cities:              {df['City'].nunique() if 'City' in df.columns else 'N/A'}")
    print(f"  Unique property types:      {df['PropertyType'].nunique() if 'PropertyType' in df.columns else 'N/A'}")
    print(f"  Close price range:          ${df['ClosePrice'].min():,.0f} – ${df['ClosePrice'].max():,.0f}")
    print(f"  Median close price:         ${df['ClosePrice'].median():,.0f}")
    print(f"  Median price/sqft:          ${df['price_per_sqft'].median():,.0f}" if 'price_per_sqft' in df.columns else "")
    print(f"  Median DOM:                 {df['DaysOnMarket'].median():.0f} days" if 'DaysOnMarket' in df.columns else "")
    print()

    # Step 5: Save final CSV
    print(f"Step 5: Saving final CSV to {FINAL_CSV} ...")
    df.to_csv(FINAL_CSV, index=False)
    print(f"  Done. {len(df):,} rows, {len(df.columns)} columns.")


if __name__ == '__main__':
    main()
