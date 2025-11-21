"""
Data ingestion module - fetch data from Dutchie POS API
"""
import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import random
from config import LOCATIONS, API_BASE_URL, MOCK_DATA_CONFIG

MOCK_DATA_DIR = "mock_data"


def fetch_pos_data(location_name, start_date, end_date, use_mock=True):
    """Fetch POS data from Dutchie API or generate mock data"""
    if use_mock:
        return generate_mock_data(location_name, start_date, end_date)
    else:
        return fetch_from_api(location_name, start_date, end_date)


def fetch_from_api(location_name, start_date, end_date):
    """Fetch data from actual Dutchie POS API"""
    location = LOCATIONS[location_name]
    api_key = location['api_key']
    
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'X-API-Key': api_key
    }
    
    try:
        possible_endpoints = [
            f"{API_BASE_URL}/v1/receipts",
            f"{API_BASE_URL}/v1/transactions", 
            f"{API_BASE_URL}/v1/orders",
            f"{API_BASE_URL}/receipts",
            f"{API_BASE_URL}/transactions",
            f"{API_BASE_URL}/orders"
        ]
        
        params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'location_id': location['id'],
            'include_items': True,
            'include_staff': True,
            'include_taxes': True
        }
        
        for endpoint in possible_endpoints:
            print(f"Trying endpoint: {endpoint}")
            response = requests.get(endpoint, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                print(f"✅ Successfully connected to {endpoint}")
                data = response.json()
                
                return parse_api_response(data, location_name)
            elif response.status_code == 401:
                print(f"❌ Authentication failed (401) - Check API key")
            elif response.status_code == 404:
                print(f"❌ Endpoint not found (404): {endpoint}")
            else:
                print(f"❌ API Error {response.status_code}: {response.text[:200]}")
        
        print("⚠️ All API endpoints failed. Falling back to mock data...")
        return generate_mock_data(location_name, start_date, end_date)
            
    except requests.exceptions.Timeout:
        print(f"⚠️ API Connection Timeout - Falling back to mock data...")
        return generate_mock_data(location_name, start_date, end_date)
    except Exception as e:
        print(f"⚠️ API Connection Error: {e}")
        print("Falling back to mock data...")
        return generate_mock_data(location_name, start_date, end_date)


def parse_api_response(data, location_name):
    """Parse the API response into our expected format"""
    orders_data = (
        data.get('receipts') or 
        data.get('orders') or 
        data.get('transactions') or 
        data.get('data', {}).get('receipts') or
        data.get('data', {}).get('orders') or
        []
    )
    
    if not orders_data:
        print(f"⚠️ No order data found in API response. Keys: {list(data.keys())}")
        print(f"Sample response: {str(data)[:500]}")
        raise ValueError("No order data in API response")
    
    orders = []
    line_items = []
    products = {}
    staff = {}
    
    for order in orders_data:
        order_record = {
            'order_id': order.get('id') or order.get('receipt_id') or order.get('order_id'),
            'location_id': order.get('location_id'),
            'location_name': location_name,
            'staff_id': order.get('staff_id') or order.get('employee_id') or order.get('cashier_id'),
            'timestamp': order.get('timestamp') or order.get('created_at') or order.get('sale_time'),
            'order_type': order.get('order_type') or order.get('type', 'in-store'),
            'is_medical': order.get('is_medical', False),
            'subtotal': order.get('subtotal', 0),
            'excise_tax': order.get('excise_tax', 0),
            'state_tax': order.get('state_tax', 0),
            'local_tax': order.get('local_tax', 0),
            'total_tax': order.get('total_tax', 0),
            'discount': order.get('discount', 0),
            'total': order.get('total', 0),
            'tender_type': order.get('tender_type') or order.get('payment_type', 'cash'),
            'voided': order.get('voided', False),
            'refunded': order.get('refunded', False),
            'promo_code': order.get('promo_code')
        }
        orders.append(order_record)
        
        items = order.get('items') or order.get('line_items') or []
        for item in items:
            line_item = {
                'line_id': item.get('id') or item.get('line_id'),
                'order_id': order_record['order_id'],
                'product_id': item.get('product_id') or item.get('sku'),
                'product_name': item.get('name') or item.get('product_name'),
                'category': item.get('category'),
                'quantity': item.get('quantity', 1),
                'unit_price': item.get('unit_price') or item.get('price', 0),
                'unit_cost': item.get('unit_cost') or item.get('cost', 0),
                'discount': item.get('discount', 0),
                'total': item.get('total', 0)
            }
            line_items.append(line_item)
            
            if item.get('product_id'):
                products[item['product_id']] = {
                    'product_id': item['product_id'],
                    'name': item.get('product_name') or item.get('name'),
                    'category': item.get('category'),
                    'subcategory': item.get('subcategory'),
                    'unit_cost': item.get('unit_cost', 0),
                    'unit_price': item.get('unit_price', 0)
                }
        
        if order.get('staff_id'):
            staff_name = order.get('staff_name') or order.get('employee_name') or f"Staff_{order['staff_id']}"
            staff[order['staff_id']] = {
                'staff_id': order['staff_id'],
                'name': staff_name
            }
    
    return {
        'orders': orders,
        'line_items': line_items,
        'products': list(products.values()),
        'staff': list(staff.values())
    }


def generate_mock_data(location_name, start_date, end_date):
    """Generate realistic mock POS data for testing"""
    location_seed = hash(location_name) % 10000
    random.seed(location_seed)
    
    location = LOCATIONS[location_name]
    days = (end_date - start_date).days + 1
    
    categories = ['Flower', 'Edibles', 'Concentrates', 'Vapes', 'Topicals', 'Accessories']
    products = []
    product_id = 1
    
    for category in categories:
        for i in range(MOCK_DATA_CONFIG['products_count'] // len(categories)):
            products.append({
                'product_id': f'prod_{product_id:04d}',
                'name': f'{category} Product {i+1}',
                'category': category,
                'subcategory': f'{category} Sub',
                'unit_cost': round(random.uniform(5, 50), 2),
                'unit_price': round(random.uniform(10, 100), 2)
            })
            product_id += 1
    
    staff = []
    for i in range(MOCK_DATA_CONFIG['staff_count']):
        staff.append({
            'staff_id': f'staff_{i+1:03d}',
            'name': f'Cashier_{i+1:03d}'
        })
    
    orders = []
    line_items = []
    order_id = 1
    line_id = 1
    
    for day in range(days):
        date = start_date + timedelta(days=day)
        num_transactions = random.randint(*MOCK_DATA_CONFIG['transactions_per_day'])
        
        for _ in range(num_transactions):
            hour = random.randint(9, 20)
            minute = random.randint(0, 59)
            timestamp = date.replace(hour=hour, minute=minute)
            
            order_type = random.choices(
                ['in-store', 'pickup', 'delivery'],
                weights=[0.6, 0.3, 0.1]
            )[0]
            
            is_medical = random.random() < 0.3
            is_voided = random.random() < 0.02
            is_refund = random.random() < 0.01
            has_discount = random.random() < 0.25
            
            num_items = random.randint(1, 5)
            order_subtotal = 0
            order_items = []
            
            for _ in range(num_items):
                product = random.choice(products)
                quantity = random.randint(1, 3)
                unit_price = product['unit_price']
                unit_cost = product['unit_cost']
                
                item_discount = 0
                if has_discount:
                    item_discount = round(unit_price * quantity * random.uniform(0.05, 0.20), 2)
                
                item_total = round(unit_price * quantity - item_discount, 2)
                order_subtotal += item_total
                
                order_items.append({
                    'line_id': f'line_{line_id:06d}',
                    'order_id': f'order_{order_id:06d}',
                    'product_id': product['product_id'],
                    'product_name': product['name'],
                    'category': product['category'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'unit_cost': unit_cost,
                    'discount': item_discount,
                    'total': item_total
                })
                line_id += 1
            
            excise_tax = round(order_subtotal * 0.10, 2)
            state_tax = round(order_subtotal * 0.06, 2)
            local_tax = round(order_subtotal * 0.02, 2)
            total_tax = excise_tax + state_tax + local_tax
            
            order_total = order_subtotal + total_tax
            
            if is_refund:
                order_total = -order_total
                order_subtotal = -order_subtotal
                total_tax = -total_tax
            
            orders.append({
                'order_id': f'order_{order_id:06d}',
                'location_id': location['id'],
                'location_name': location_name,
                'staff_id': random.choice(staff)['staff_id'],
                'timestamp': timestamp.isoformat(),
                'order_type': order_type,
                'is_medical': is_medical,
                'subtotal': order_subtotal,
                'excise_tax': excise_tax if not is_refund else -excise_tax,
                'state_tax': state_tax if not is_refund else -state_tax,
                'local_tax': local_tax if not is_refund else -local_tax,
                'total_tax': total_tax,
                'discount': sum(item['discount'] for item in order_items),
                'total': order_total,
                'tender_type': random.choice(['cash', 'credit', 'debit', 'debit']),
                'voided': is_voided,
                'refunded': is_refund,
                'promo_code': f'PROMO{random.randint(1,5)}' if has_discount and random.random() < 0.5 else None
            })
            
            line_items.extend(order_items)
            order_id += 1
    
    mock_data = {
        'orders': orders,
        'line_items': line_items,
        'products': products,
        'staff': staff
    }
    
    save_mock_data_to_csv(mock_data, location_name, start_date, end_date)
    
    return mock_data


def save_mock_data_to_csv(mock_data, location_name, start_date, end_date):
    """Save mock data as a realistic Dutchie POS export CSV file"""
    Path(MOCK_DATA_DIR).mkdir(exist_ok=True)
    
    location_safe = location_name.replace(' ', '_')
    filename = f"{location_safe}_transactions.csv"
    filepath = os.path.join(MOCK_DATA_DIR, filename)
    
    df_orders = pd.DataFrame(mock_data['orders'])
    df_line_items = pd.DataFrame(mock_data['line_items'])
    df_products = pd.DataFrame(mock_data['products'])
    df_staff = pd.DataFrame(mock_data['staff'])
    
    pos_export_rows = []
    
    for _, order in df_orders.iterrows():
        order_items = df_line_items[df_line_items['order_id'] == order['order_id']]
        staff_info = df_staff[df_staff['staff_id'] == order['staff_id']].iloc[0] if len(df_staff[df_staff['staff_id'] == order['staff_id']]) > 0 else {'name': 'Unknown'}
        
        for _, item in order_items.iterrows():
            product = df_products[df_products['product_id'] == item['product_id']].iloc[0] if len(df_products[df_products['product_id'] == item['product_id']]) > 0 else {}
            
            pos_export_rows.append({
                'transaction_id': order['order_id'],
                'transaction_date': order['timestamp'],
                'location_name': order['location_name'],
                'location_id': order['location_id'],
                'employee_id': order['staff_id'],
                'employee_name': staff_info.get('name', 'Unknown'),
                'order_type': order['order_type'],
                'is_medical': order['is_medical'],
                'product_id': item['product_id'],
                'product_name': product.get('name', item.get('product_name', 'Unknown')),
                'category': product.get('category', 'Other'),
                'subcategory': product.get('subcategory', ''),
                'quantity': item['quantity'],
                'unit_price': item['unit_price'],
                'unit_cost': item['unit_cost'],
                'item_discount': item['discount'],
                'item_total': item['total'],
                'order_subtotal': order['subtotal'],
                'excise_tax': order['excise_tax'],
                'state_tax': order['state_tax'],
                'local_tax': order['local_tax'],
                'total_tax': order['total_tax'],
                'order_discount': order['discount'],
                'order_total': order['total'],
                'tender_type': order['tender_type'],
                'voided': order['voided'],
                'refunded': order['refunded'],
                'promo_code': order.get('promo_code', '')
            })
    
    pos_export_df = pd.DataFrame(pos_export_rows)
    pos_export_df.to_csv(filepath, index=False)
    
    print(f"✅ Saved POS export for {location_name}: {filepath}")
    print(f"   - {len(mock_data['orders'])} transactions")
    print(f"   - {len(pos_export_rows)} line items")
    print(f"   - {len(mock_data['products'])} unique products")
    print(f"   - {len(mock_data['staff'])} staff members")


def load_data_for_all_locations(start_date, end_date, use_mock=True):
    """Load data for all configured locations with API keys"""
    all_data = {
        'orders': [],
        'line_items': [],
        'products': [],
        'staff': []
    }
    
    for location_name, config in LOCATIONS.items():
        if config.get('api_key'):
            print(f"Fetching data for {location_name}...")
            location_data = fetch_pos_data(location_name, start_date, end_date, use_mock)
            
            all_data['orders'].extend(location_data['orders'])
            all_data['line_items'].extend(location_data['line_items'])
            all_data['products'].extend(location_data['products'])
            all_data['staff'].extend(location_data['staff'])
    
    return all_data
