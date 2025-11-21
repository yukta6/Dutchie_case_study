"""
File upload and parsing module for Dutchie POS data
Handles CSV uploads from POS exports
"""
import pandas as pd
import json
from datetime import datetime
import streamlit as st
from difflib import get_close_matches


def find_column(df, possible_names, fuzzy=True):
    """
    Find column by checking multiple possible names (case-insensitive)
    Falls back to fuzzy matching if exact match not found
    
    Args:
        df: DataFrame to search
        possible_names: List of possible column names
        fuzzy: Whether to use fuzzy matching as fallback
        
    Returns:
        Column name if found, None otherwise
    """
    df_columns_lower = {col.lower(): col for col in df.columns}
    
    # Try exact match first
    for name in possible_names:
        if name.lower() in df_columns_lower:
            return df_columns_lower[name.lower()]
    
    # Fallback to fuzzy matching if enabled
    if fuzzy:
        all_columns = list(df.columns)
        for name in possible_names:
            # Find close matches (80% similarity)
            matches = get_close_matches(name.lower(), [c.lower() for c in all_columns], n=1, cutoff=0.6)
            if matches:
                # Return the original column name (not lowercase)
                matched_col = [c for c in all_columns if c.lower() == matches[0]][0]
                return matched_col
    
    return None
    """
    Parse uploaded CSV file from Dutchie POS export
    
    Args:
        uploaded_file: Streamlit UploadedFile object
        location_name: Name to assign to this location
        
    Returns:
        dict with keys: orders, line_items, products, staff
    """
    try:
        file_extension = uploaded_file.name.split('.')[-1].lower()
        
        if file_extension == 'csv':
            return parse_csv_file(uploaded_file, location_name)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}. Please upload CSV.")
            
    except Exception as e:
        import traceback
        st.error(f"Error parsing file: {str(e)}")
        st.error(f"Traceback: {traceback.format_exc()}")
        raise


def parse_csv_file(uploaded_file, location_name):
    """Parse CSV file with POS transaction data"""
    df = pd.read_csv(uploaded_file)
    
    def find_column(df, patterns):
        """Find the first column matching any of the patterns"""
        df_columns_lower = [col.lower() for col in df.columns]
        for pattern in patterns:
            pattern_lower = pattern.lower()
            for i, col in enumerate(df_columns_lower):
                if col == pattern_lower:
                    return df.columns[i]
            for i, col in enumerate(df_columns_lower):
                if pattern_lower in col or col in pattern_lower:
                    return df.columns[i]
        return None
    
    column_mapping = {}
    
    order_id_col = find_column(df, ['transaction_id', 'order_id', 'transactionid', 'receipt_id', 'receiptid', 'id'])
    if order_id_col:
        column_mapping[order_id_col] = 'order_id'
    
    timestamp_col = find_column(df, ['transaction_date', 'timestamp', 'transactiondate', 'created_at', 'date', 'datetime', 'sale_time'])
    if timestamp_col:
        column_mapping[timestamp_col] = 'timestamp'
    
    staff_col = find_column(df, ['employee_id', 'staff_id', 'employeeid', 'cashier_id', 'cashierid', 'responsible', 'user_id'])
    if staff_col:
        column_mapping[staff_col] = 'staff_id'
    
    staff_name_col = find_column(df, ['employee_name', 'staff_name', 'employeename', 'cashier_name'])
    if staff_name_col:
        column_mapping[staff_name_col] = 'staff_name'
    
    product_id_col = find_column(df, ['product_id', 'productid', 'sku', 'item_id'])
    if product_id_col:
        column_mapping[product_id_col] = 'product_id'
    
    category_col = find_column(df, ['category', 'product_category', 'item_category'])
    if category_col:
        column_mapping[category_col] = 'category'
    
    quantity_col = find_column(df, ['quantity', 'qty', 'item_quantity'])
    if quantity_col:
        column_mapping[quantity_col] = 'quantity'
    
    unit_price_col = find_column(df, ['unit_price', 'unitprice', 'price', 'item_price'])
    if unit_price_col:
        column_mapping[unit_price_col] = 'unit_price'
    
    unit_cost_col = find_column(df, ['unit_cost', 'unitcost', 'cost', 'item_cost'])
    if unit_cost_col:
        column_mapping[unit_cost_col] = 'unit_cost'
    
    item_discount_col = find_column(df, ['item_discount', 'discount', 'total_discount', 'totaldiscount'])
    if item_discount_col:
        column_mapping[item_discount_col] = 'item_discount'
    
    order_discount_col = find_column(df, ['order_discount', 'total_discount'])
    if order_discount_col:
        column_mapping[order_discount_col] = 'order_discount'
    
    item_total_col = find_column(df, ['item_total', 'total', 'amount', 'totalprice'])
    if item_total_col:
        column_mapping[item_total_col] = 'item_total'
    
    order_total_col = find_column(df, ['order_total', 'total_amount'])
    if order_total_col:
        column_mapping[order_total_col] = 'order_total'
    
    order_subtotal_col = find_column(df, ['order_subtotal', 'subtotal', 'sub_total', 'beforetax'])
    if order_subtotal_col:
        column_mapping[order_subtotal_col] = 'order_subtotal'
    
    tax_col = find_column(df, ['tax', 'total_tax', 'totaltax', 'taxes'])
    if tax_col:
        column_mapping[tax_col] = 'total_tax'
    
    order_type_col = find_column(df, ['order_type', 'ordertype', 'type', 'channel'])
    if order_type_col:
        column_mapping[order_type_col] = 'order_type'
    
    medical_col = find_column(df, ['is_medical', 'ismedical', 'medical'])
    if medical_col:
        column_mapping[medical_col] = 'is_medical'
    
    void_col = find_column(df, ['voided', 'is_void', 'isvoid', 'void'])
    if void_col:
        column_mapping[void_col] = 'voided'
    
    df.rename(columns=column_mapping, inplace=True)
    required_columns = ['order_id', 'timestamp']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        available = list(df.columns[:10])  # Show first 10 columns
        raise ValueError(
            f"Could not find required fields. Need: {required_columns}\n"
            f"Available columns: {available}...\n"
            f"Please ensure your CSV has transaction ID and date/time columns."
        )
    
    location_id = f"LOC_{location_name.upper().replace(' ', '_')}"
    
    def safe_get(row, column, default, convert_type=None):
        """Safely extract value from row with fallback"""
        if column not in df.columns:
            return default
        
        # Get value - handle both Series and dict-like access
        try:
            if isinstance(row, pd.Series):
                val = row[column]
            else:
                val = row.get(column, default)
        except (KeyError, AttributeError):
            return default
        
        # Handle Series that might be returned
        if isinstance(val, pd.Series):
            if len(val) > 0:
                val = val.iloc[0]
            else:
                return default
        
        # Check if it's NaN - now val should be scalar
        try:
            if pd.isna(val):
                return default
        except (TypeError, ValueError):
            pass
            
        if convert_type:
            try:
                return convert_type(val)
            except:
                return default
        return val
    
    orders = []
    line_items = []
    products = {}
    staff = {}
    
    grouped = df.groupby('order_id')
    
    for order_id, group in grouped:
        first_row = group.iloc[0]
        
        tender_type = 'cash'
        
        if 'tender_type' in df.columns and pd.notna(first_row.get('tender_type')):
            tender_type = str(first_row['tender_type']).lower()
        elif 'tender_type' not in df.columns:
            for col in df.columns:
                col_lower = col.lower()
                val = first_row.get(col, 0)
                if pd.notna(val) and isinstance(val, (int, float)) and val > 0:
                    if 'credit' in col_lower:
                        tender_type = 'credit'
                        break
                    elif 'debit' in col_lower:
                        tender_type = 'debit'
                        break
                    elif 'cash' in col_lower:
                        tender_type = 'cash'
                        break
        
        order = {
            'order_id': str(order_id),
            'location_id': location_id,
            'location_name': location_name,
            'staff_id': str(safe_get(first_row, 'staff_id', 'unknown')),
            'timestamp': str(safe_get(first_row, 'timestamp', datetime.now().isoformat())),
            'order_type': safe_get(first_row, 'order_type', 'in-store', str),
            'is_medical': safe_get(first_row, 'is_medical', False, bool),
            'subtotal': safe_get(first_row, 'order_subtotal', safe_get(first_row, 'subtotal', 0, float), float),
            'excise_tax': safe_get(first_row, 'excise_tax', 0, float),
            'state_tax': safe_get(first_row, 'state_tax', 0, float),
            'local_tax': safe_get(first_row, 'local_tax', 0, float),
            'total_tax': safe_get(first_row, 'total_tax', 0, float),
            'discount': safe_get(first_row, 'order_discount', safe_get(first_row, 'discount', 0, float), float),
            'total': safe_get(first_row, 'order_total', safe_get(first_row, 'total', 0, float), float),
            'tender_type': tender_type,
            'voided': safe_get(first_row, 'voided', False, bool),
            'refunded': safe_get(first_row, 'refunded', False, bool),
            'promo_code': safe_get(first_row, 'promo_code', None)
        }
        orders.append(order)
        
        for idx, row in group.iterrows():
            product_id = safe_get(row, 'product_id', f'prod_{idx}')
            
            line_item = {
                'line_id': f'line_{idx}',
                'order_id': str(order_id),
                'product_id': str(product_id),
                'product_name': safe_get(row, 'product_name', 'Unknown Product', str),
                'category': safe_get(row, 'category', 'Other', str),
                'quantity': safe_get(row, 'quantity', 1, float),
                'unit_price': safe_get(row, 'unit_price', 0, float),
                'unit_cost': safe_get(row, 'unit_cost', 0, float),
                'discount': safe_get(row, 'item_discount', safe_get(row, 'discount', 0, float), float),
                'total': safe_get(row, 'item_total', safe_get(row, 'total', 0, float), float)
            }
            line_items.append(line_item)
            
            products[product_id] = {
                'product_id': str(product_id),
                'name': safe_get(row, 'product_name', 'Unknown Product', str),
                'category': safe_get(row, 'category', 'Other', str),
                'subcategory': safe_get(row, 'subcategory', '', str),
                'unit_cost': safe_get(row, 'unit_cost', 0, float),
                'unit_price': safe_get(row, 'unit_price', 0, float)
            }
        
        staff_id = safe_get(first_row, 'staff_id', 'unknown')
        staff_id_str = str(staff_id) if staff_id != 'unknown' else 'unknown'
        if staff_id_str and staff_id_str != 'unknown' and staff_id_str not in staff:
            staff[staff_id_str] = {
                'staff_id': staff_id_str,
                'name': safe_get(first_row, 'staff_name', f'Staff_{staff_id_str}', str)
            }
    
    return {
        'orders': orders,
        'line_items': line_items,
        'products': list(products.values()),
        'staff': list(staff.values())
    }


def parse_json_file(uploaded_file, location_name):
    """Parse JSON file with POS transaction data"""
    data = json.load(uploaded_file)
    
    location_id = f"LOC_{location_name.upper().replace(' ', '_')}"
    
    if 'orders' in data and 'line_items' in data:
        for order in data['orders']:
            order['location_id'] = location_id
            order['location_name'] = location_name
        return data
    orders_data = data.get('orders') or data.get('receipts') or data.get('transactions') or []
    
    orders = []
    line_items = []
    products = {}
    staff = {}
    
    for order in orders_data:
        order_id = order.get('id') or order.get('order_id') or order.get('receipt_id')
        
        order_record = {
            'order_id': str(order_id),
            'location_id': location_id,
            'location_name': location_name,
            'staff_id': order.get('staff_id', order.get('employee_id', 'unknown')),
            'timestamp': order.get('timestamp', order.get('created_at', datetime.now().isoformat())),
            'order_type': order.get('order_type', order.get('type', 'in-store')),
            'is_medical': order.get('is_medical', False),
            'subtotal': order.get('subtotal', 0),
            'excise_tax': order.get('excise_tax', 0),
            'state_tax': order.get('state_tax', 0),
            'local_tax': order.get('local_tax', 0),
            'total_tax': order.get('total_tax', order.get('tax', 0)),
            'discount': order.get('discount', 0),
            'total': order.get('total', 0),
            'tender_type': order.get('tender_type', order.get('payment_type', 'cash')),
            'voided': order.get('voided', False),
            'refunded': order.get('refunded', False),
            'promo_code': order.get('promo_code')
        }
        orders.append(order_record)
        
        items = order.get('items') or order.get('line_items') or []
        for item in items:
            product_id = item.get('product_id', item.get('sku', item.get('id')))
            
            line_item = {
                'line_id': item.get('id', item.get('line_id', f'line_{len(line_items)}')),
                'order_id': str(order_id),
                'product_id': str(product_id),
                'product_name': item.get('name', item.get('product_name', 'Unknown')),
                'category': item.get('category', 'Other'),
                'quantity': item.get('quantity', 1),
                'unit_price': item.get('unit_price', item.get('price', 0)),
                'unit_cost': item.get('unit_cost', item.get('cost', 0)),
                'discount': item.get('discount', 0),
                'total': item.get('total', 0)
            }
            line_items.append(line_item)
            
            products[product_id] = {
                'product_id': str(product_id),
                'name': item.get('name', item.get('product_name', 'Unknown')),
                'category': item.get('category', 'Other'),
                'subcategory': item.get('subcategory', ''),
                'unit_cost': item.get('unit_cost', 0),
                'unit_price': item.get('unit_price', 0)
            }
        
        staff_id = order.get('staff_id', order.get('employee_id'))
        if staff_id and staff_id not in staff:
            staff[staff_id] = {
                'staff_id': str(staff_id),
                'name': order.get('staff_name', order.get('employee_name', f'Staff_{staff_id}'))
            }
    
    return {
        'orders': orders,
        'line_items': line_items,
        'products': list(products.values()),
        'staff': list(staff.values())
    }


def parse_uploaded_file(uploaded_file, location_name):
    """
    Parse uploaded file (CSV or JSON) from Dutchie POS export
    
    Args:
        uploaded_file: Streamlit UploadedFile object
        location_name: Name to assign to this location
        
    Returns:
        Dictionary with orders, line_items, products, staff lists
    """
    file_name = uploaded_file.name.lower()
    
    if file_name.endswith('.json'):
        return parse_json_file(uploaded_file, location_name)
    elif file_name.endswith('.csv'):
        return parse_csv_file(uploaded_file, location_name)
    else:
        raise ValueError(f"Unsupported file type: {file_name}. Please upload CSV or JSON files.")


def validate_uploaded_data(data):
    """Validate that uploaded data has required structure"""
    required_keys = ['orders', 'line_items', 'products', 'staff']
    
    for key in required_keys:
        if key not in data:
            return False, f"Missing required key: {key}"
        if not isinstance(data[key], list):
            return False, f"{key} must be a list"
    
    if len(data['orders']) == 0:
        return False, "No orders found in uploaded file"
    
    required_order_fields = ['order_id', 'timestamp', 'total']
    first_order = data['orders'][0]
    missing_fields = [f for f in required_order_fields if f not in first_order]
    
    if missing_fields:
        return False, f"Orders missing required fields: {missing_fields}"
    
    return True, "Data validation successful"
