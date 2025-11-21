"""
Data cleaning and transformation module
"""
import pandas as pd
import pytz
from datetime import datetime
from config import LOCATIONS, DAYPARTS, THRESHOLDS


def clean_data(raw_data):
    """Clean and normalize raw POS data"""
    df_orders = pd.DataFrame(raw_data['orders'])
    df_line_items = pd.DataFrame(raw_data['line_items'])
    df_products = pd.DataFrame(raw_data['products'])
    df_staff = pd.DataFrame(raw_data['staff'])
    
    df_orders = clean_orders(df_orders)
    df_line_items = clean_line_items(df_line_items)
    df_products = clean_products(df_products)
    df_staff = clean_staff(df_staff)
    
    return {
        'orders': df_orders,
        'line_items': df_line_items,
        'products': df_products,
        'staff': df_staff
    }


def clean_orders(df):
    """Clean and normalize orders data"""
    df = df.copy()
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    df['timestamp_local'] = df.apply(
        lambda row: convert_to_local_time(row['timestamp'], row['location_name']),
        axis=1
    )
    
    df['date'] = df['timestamp_local'].dt.date
    df['hour'] = df['timestamp_local'].dt.hour
    df['day_of_week'] = df['timestamp_local'].dt.day_name()
    
    df['daypart'] = df['hour'].apply(get_daypart)
    df['order_type'] = df['order_type'].str.lower().str.strip()
    df['order_type'] = df['order_type'].replace({
        'in-store': 'in_store',
        'instore': 'in_store',
        'in store': 'in_store'
    })
    
    df['tender_type'] = df['tender_type'].str.lower().str.strip()
    
    df['discount_rate'] = 0.0
    if 'subtotal' in df.columns:
        mask = df['subtotal'] > 0
        df.loc[mask, 'discount_rate'] = (df.loc[mask, 'discount'] / df.loc[mask, 'subtotal'] * 100).round(2)
    else:
        df['reconstructed_subtotal'] = df['total'] + df['discount']
        mask = df['reconstructed_subtotal'] > 0
        df.loc[mask, 'discount_rate'] = (df.loc[mask, 'discount'] / df.loc[mask, 'reconstructed_subtotal'] * 100).round(2)
        df = df.drop(columns=['reconstructed_subtotal'])
    
    df['discount_rate'] = df['discount_rate'].clip(-100, 100)
    
    df['time_id'] = df['timestamp_local'].dt.strftime('%Y%m%d%H')
    
    return df


def clean_line_items(df):
    """Clean and normalize line items data"""
    df = df.copy()
    
    df['product_name'] = df['product_name'].str.strip().str.lower()
    df['category'] = df['category'].str.strip().str.title()
    df['margin'] = (df['unit_price'] - df['unit_cost']) * df['quantity']
    
    return df


def clean_products(df):
    """Clean and normalize products data"""
    df = df.copy()
    
    df['name'] = df['name'].str.strip().str.lower()
    df['category'] = df['category'].str.strip().str.title()
    df['subcategory'] = df['subcategory'].str.strip().str.title()
    df = df.drop_duplicates(subset=['product_id'], keep='first')
    
    return df


def clean_staff(df):
    """Clean and normalize staff data"""
    df = df.copy()
    
    df['name'] = df['staff_id'].apply(lambda x: f"Cashier_{x.split('_')[-1]}")
    df = df.drop_duplicates(subset=['staff_id'], keep='first')
    
    return df


def convert_to_local_time(timestamp, location_name):
    """Convert timestamp to store-local time
    
    This function is flexible and handles:
    - Naive timestamps (no timezone) - assumes they are already in local store time
    - UTC timestamps - converts to store local time
    - Any other timezone - converts to store local time
    """
    from config import get_location_config
    
    location_config = get_location_config(location_name)
    timezone_str = location_config['timezone']
    location_tz = pytz.timezone(timezone_str)
    
    if timestamp.tzinfo is None:
        # Assume naive timestamps are already in local store time
        timestamp = location_tz.localize(timestamp)
    else:
        # Convert timezone-aware timestamps to store local time
        timestamp = timestamp.astimezone(location_tz)
    
    return timestamp


def get_daypart(hour):
    """Determine daypart from hour"""
    for daypart_name, (start, end) in DAYPARTS.items():
        if start <= hour < end:
            return daypart_name
    return 'Other'


def detect_exceptions(df_orders):
    """Detect exceptions and anomalies in orders data"""
    exceptions = []
    
    negative_mask = df_orders['total'] < THRESHOLDS['negative_total_threshold']
    for _, row in df_orders[negative_mask].iterrows():
        if not row['refunded']:
            exceptions.append({
                'type': 'negative_total',
                'order_id': row['order_id'],
                'location': row['location_name'],
                'timestamp': row['timestamp_local'],
                'value': row['total'],
                'description': f"Negative total: ${row['total']:.2f}"
            })
    
    high_discount_mask = df_orders['discount_rate'] > THRESHOLDS['discount_rate_high']
    for _, row in df_orders[high_discount_mask].iterrows():
        exceptions.append({
            'type': 'high_discount',
            'order_id': row['order_id'],
            'location': row['location_name'],
            'timestamp': row['timestamp_local'],
            'value': row['discount_rate'],
            'description': f"High discount rate: {row['discount_rate']:.1f}%"
        })
    
    df_orders['calculated_tax'] = df_orders['excise_tax'] + df_orders['state_tax'] + df_orders['local_tax']
    df_orders['tax_diff'] = abs(df_orders['total_tax'] - df_orders['calculated_tax'])
    
    tax_mismatch_mask = df_orders['tax_diff'] > 0.05
    for _, row in df_orders[tax_mismatch_mask].iterrows():
        exceptions.append({
            'type': 'tax_mismatch',
            'order_id': row['order_id'],
            'location': row['location_name'],
            'timestamp': row['timestamp_local'],
            'value': row['tax_diff'],
            'description': f"Tax mismatch: ${row['tax_diff']:.2f}"
        })
    
    daily_voids = df_orders.groupby(['date', 'location_name'])['voided'].sum()
    daily_median_voids = df_orders.groupby('location_name')['voided'].transform('median')
    
    staff_voids = df_orders.groupby('staff_id').agg({
        'voided': 'sum',
        'order_id': 'count'
    })
    staff_voids['void_rate'] = staff_voids['voided'] / staff_voids['order_id'] * 100
    
    high_void_staff = staff_voids[staff_voids['void_rate'] > 5.0]
    for staff_id, row in high_void_staff.iterrows():
        exceptions.append({
            'type': 'high_void_rate',
            'order_id': None,
            'location': 'All',
            'timestamp': None,
            'value': row['void_rate'],
            'description': f"Staff {staff_id}: {row['void_rate']:.1f}% void rate"
        })
    
    return pd.DataFrame(exceptions)


def validate_data_quality(cleaned_data):
    """Validate data quality and return quality report"""
    df_orders = cleaned_data['orders']
    df_line_items = cleaned_data['line_items']
    
    report = {
        'total_orders': len(df_orders),
        'total_line_items': len(df_line_items),
        'date_range': (df_orders['date'].min(), df_orders['date'].max()),
        'locations': df_orders['location_name'].nunique(),
        'void_rate': (df_orders['voided'].sum() / len(df_orders) * 100),
        'refund_rate': (df_orders['refunded'].sum() / len(df_orders) * 100),
        'avg_discount_rate': df_orders['discount_rate'].mean(),
        'negative_totals': len(df_orders[df_orders['total'] < 0]),
        'missing_values': {
            'orders': df_orders.isnull().sum().to_dict(),
            'line_items': df_line_items.isnull().sum().to_dict()
        }
    }
    
    return report
