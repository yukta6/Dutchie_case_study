# Dutchie POS Dashboard

A manager-focused analytics dashboard for Dutchie POS data. Upload CSV files and get instant insights.

## How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Start dashboard
streamlit run app.py
# OR
python -m streamlit run app.py
```

Open **http://localhost:8501** and upload your CSV transaction files.

### Generate Mock Data 
```bash
# Generate 28 days (4 weeks) of test data
python3 generate_mock_csv.py 
```

Files are saved to `mock_data` folder. Upload them through the dashboard interface.

## Assumptions

### Data Quality
- **Timestamps**: Provided in UTC or naive format (treated as local store time)
- **Order IDs**: Unique within each location
- **Negative totals**: Only allowed for refunded orders
- **Missing cost data**: Assumed 50% gross margin when unit_cost is unavailable

### Business Logic
- **Dayparts**: Morning (9-12), Afternoon (12-5), Evening (5-9)
- **Voided orders**: Cancelled transactions - excluded from revenue calculations
- **Refunded orders**: Returned purchases - included as negative revenue
- **Tax calculation**: `total_tax = excise_tax + state_tax + local_tax`
- **Void spike threshold**: Daily voids >2× median indicate training needs

### Technical
- **Timezone handling**: One timezone per location (configured in `config.py`)
- **Database**: Local DuckDB file (`dutchie_pos.db`) - data persists between restarts
- **File format flexibility**: Smart column mapping handles varying CSV structures
- **Privacy**: Staff names pseudonymized automatically (no customer PII stored)

## Data Cleaning Rules

### 1. Column Mapping (Flexible)
Automatically detects and maps common column name variations:
- **Order ID**: `transaction_id`, `order_id`, `receiptid`, `id`
- **Timestamp**: `transaction_date`, `timestamp`, `created_at`, `date`
- **Staff**: `employee_id`, `staff_id`, `cashier_id`, `user_id`
- **Category**: `category`, `product_category`, `item_category`
- **Price**: `unit_price`, `price`, `item_price`

### 2. Normalization
- **Products**: Lowercase + strip whitespace
- **Categories**: Title case (e.g., "Flower", "Edibles")
- **Order types**: Standardized to `in_store`, `pickup`, `delivery`
- **Tenders**: Lowercase (`credit`, `debit`, `cash`)

### 3. Timezone Conversion
```python
# Naive timestamps → treated as local store time
# UTC timestamps → converted to local store time
# Timezone-aware → converted to local store time
```

### 4. Derived Fields
- **Date**: Extracted from timestamp
- **Hour**: 0-23 for hourly analysis
- **Day of week**: Monday, Tuesday, etc.
- **Daypart**: Morning/Afternoon/Evening based on hour
- **Margin**: `(unit_price - unit_cost) × quantity`

### 5. Exception Detection
Automatically flags these issues in the Compliance Panel:
- **Negative totals** (unless order is refunded)
- **Tax mismatches** (calculated tax ≠ reported tax)
- **Void spikes** (daily voids >2× median)
- **High discounts** (discount rate >30%)
- **Orphan refunds** (refund with no matching original sale)

### 6. Privacy & Compliance
- Staff names → `Cashier_001`, `Cashier_002`, etc.
- No customer PII collected or stored

## Things that can be done next

### 1. Automated Alerting
**Problem**: Managers must manually check dashboard daily  
**Solution**:
- Email/SMS alerts when:
  - Void rate >2× baseline
  - Tax mismatch >$50
  - Daily sales drop >15% vs previous week
- Scheduled daily summary email at 8am with key metrics

### 2. Multi-Month Trend Analysis
**Problem**: Current data reloads overwrite history  
**Solution**:
- PostgreSQL/Snowflake backend for persistent storage
- Month-over-month comparison views
- 12-month rolling trends for seasonality analysis

### 3. Forecasting & Predictive Analytics
**Problem**: Dashboard is reactive, not proactive  
**Solution**:
- Linear regression to predict next week's sales
- Anomaly detection using statistical control charts
- Inventory reorder recommendations based on velocity trends

---

## Tech Stack
- **Frontend**: Streamlit 1.29.0
- **Data Processing**: Pandas 2.1.4
- **Visualization**: Plotly 5.18.0
- **Database**: DuckDB 1.0.0 (local analytics DB)
- **Timezone**: pytz 2023.3

