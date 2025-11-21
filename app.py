"""
Streamlit Dashboard for Dutchie POS Analytics
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import time

from data_ingestion import load_data_for_all_locations
from data_cleaning import clean_data, detect_exceptions, validate_data_quality
from database import DutchieDB
from config import LOCATIONS, DAYPARTS
from file_upload import parse_uploaded_file, validate_uploaded_data

st.set_page_config(
    page_title="Dutchie POS Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .kpi-label {
        font-size: 0.9rem;
        color: #666;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .exception-warning {
        background-color: #fff3cd;
        padding: 0.5rem;
        border-left: 4px solid #ff9800;
        margin: 0.5rem 0;
        color: #856404;
    }
    .exception-error {
        background-color: #f8d7da;
        padding: 0.5rem;
        border-left: 4px solid #dc3545;
        margin: 0.5rem 0;
        color: #721c24;
        font-weight: 500;
    }
    .empty-state {
        text-align: center;
        padding: 3rem 1rem;
        background-color: #f8f9fa;
        border-radius: 0.5rem;
        margin: 2rem 0;
    }
    .empty-state-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
    }
    .empty-state-text {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 1rem;
    }
    
    /* Mobile-friendly responsive design for ~400px width */
    @media screen and (max-width: 768px) {
        .main-header {
            font-size: 1.8rem;
        }
        .kpi-value {
            font-size: 1.5rem;
        }
        .row-widget.stHorizontal {
            flex-direction: column !important;
        }
        .dataframe {
            font-size: 0.8rem;
        }
    }
    
    @media screen and (max-width: 400px) {
        .main-header {
            font-size: 1.5rem;
        }
        .kpi-value {
            font-size: 1.2rem;
        }
        body {
            font-size: 0.9rem;
        }
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)
def load_and_process_data(start_date, end_date, use_mock=True):
    """Load and process POS data with caching"""
    raw_data = load_data_for_all_locations(start_date, end_date, use_mock)
    cleaned_data = clean_data(raw_data)
    exceptions = detect_exceptions(cleaned_data['orders'])
    quality_report = validate_data_quality(cleaned_data)
    
    return cleaned_data, exceptions, quality_report


def initialize_database(cleaned_data):
    """Initialize and load database"""
    db = DutchieDB()
    db.connect()
    db.create_schema()
    db.load_data(cleaned_data)
    return db


def render_filters(df_orders, df_line_items):
    """Render sidebar filters"""
    
    st.sidebar.header("Filters")
    
    filters = {}
    
    min_date = df_orders['date'].min()
    max_date = df_orders['date'].max()
    
    if hasattr(min_date, 'date'):
        min_date = min_date.date()
    if hasattr(max_date, 'date'):
        max_date = max_date.date()
    
    today = datetime.now().date()
    max_selectable_date = min(max_date, today)
    
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_selectable_date),
        max_value=today,
        key="date_range_filter",
        help=f"Data available from {min_date} to {max_date}"
    )
    
    if len(date_range) == 2:
        filters['start_date'] = date_range[0]
        filters['end_date'] = date_range[1]
    else:
        filters['start_date'] = min_date
        filters['end_date'] = max_date
    
    all_locations = df_orders['location_name'].unique()
    
    locations = st.sidebar.multiselect(
        "Location(s)",
        options=all_locations,
        default=list(all_locations)
    )
    
    if locations:
        location_ids = df_orders[df_orders['location_name'].isin(locations)]['location_id'].unique().tolist()
        filters['locations'] = location_ids
    
    order_type = st.sidebar.selectbox(
        "Order Type",
        options=['All'] + list(df_orders['order_type'].unique()),
        index=0
    )
    if order_type != 'All':
        filters['order_type'] = order_type
    
    daypart = st.sidebar.selectbox(
        "Daypart",
        options=['All'] + list(DAYPARTS.keys()),
        index=0,
        help=f"Available in data: {sorted(df_orders['daypart'].unique())}"
    )
    if daypart != 'All':
        filters['daypart'] = daypart
    
    available_categories = sorted(df_line_items['category'].unique())
    category = st.sidebar.selectbox(
        "Category",
        options=['All'] + available_categories,
        index=0
    )
    if category != 'All':
        filters['category'] = category
    
    staff_options = df_orders['staff_id'].unique()
    staff = st.sidebar.selectbox(
        "Cashier",
        options=['All'] + list(staff_options),
        index=0
    )
    if staff != 'All':
        filters['staff_id'] = staff
    
    return filters


def apply_filters(df, filters, is_line_items=False):
    """Apply filters to dataframe"""
    filtered_df = df.copy()
    
    if is_line_items:
        if 'category' in filters:
            filtered_df = filtered_df[filtered_df['category'] == filters['category']]
        return filtered_df
    
    if 'start_date' in filters and 'end_date' in filters:
        filtered_df = filtered_df[
            (filtered_df['date'] >= filters['start_date']) &
            (filtered_df['date'] <= filters['end_date'])
        ]
    
    if 'locations' in filters and filters['locations']:
        filtered_df = filtered_df[filtered_df['location_id'].isin(filters['locations'])]
    
    if 'order_type' in filters:
        filtered_df = filtered_df[filtered_df['order_type'] == filters['order_type']]
    
    if 'daypart' in filters:
        filtered_df = filtered_df[filtered_df['daypart'] == filters['daypart']]
    
    if 'staff_id' in filters:
        filtered_df = filtered_df[filtered_df['staff_id'] == filters['staff_id']]
    
    return filtered_df


def render_kpi_cards(df_orders, df_line_items):
    """Render main KPI cards"""
    df_sales = df_orders[~df_orders['voided']].copy()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        net_sales = df_sales['total'].sum()
        st.metric(
            "Total Net Sales", 
            f"${net_sales:,.2f}",
            help="Sum of all order totals (excluding voided transactions). Formula: Σ(order.total) where voided=false"
        )
    
    with col2:
        total_orders = len(df_sales)
        st.metric(
            "Total Transactions", 
            f"{total_orders:,}",
            help="Number of completed orders (excluding voids). Formula: COUNT(orders) where voided=false"
        )
    
    with col3:
        total_margin = df_line_items['margin'].sum() if 'margin' in df_line_items.columns else 0
        margin_pct = (total_margin / net_sales * 100) if net_sales > 0 else 0
        st.metric(
            "Gross Margin %", 
            f"{margin_pct:.1f}%",
            help="Gross margin as percentage of sales. Formula: (Σ(unit_price - unit_cost) × quantity) / total_sales × 100"
        )
    
    with col4:
        total_tax = df_sales['total_tax'].sum()
        st.metric(
            "Total Tax Collected", 
            f"${total_tax:,.2f}",
            help="Sum of all taxes (excise + state + local). Formula: Σ(excise_tax + state_tax + local_tax)"
        )


def render_sales_comparison(df_orders, filters):
    """Render week-over-week sales comparison"""
    df_sales = df_orders[~df_orders['voided']].copy()
    
    current_sales = df_sales['total'].sum()
    current_orders = len(df_sales)
    
    all_orders = st.session_state.get('all_orders', df_orders)
    
    days_diff = (filters['end_date'] - filters['start_date']).days + 1
    prev_start = filters['start_date'] - timedelta(days=days_diff)
    prev_end = filters['start_date'] - timedelta(days=1)
    
    df_prev = all_orders[
        (all_orders['date'] >= prev_start) &
        (all_orders['date'] <= prev_end) &
        (~all_orders['voided'])
    ]
    
    if 'locations' in filters and filters['locations']:
        df_prev = df_prev[df_prev['location_id'].isin(filters['locations'])]
    
    prev_sales = df_prev['total'].sum() if len(df_prev) > 0 else 0
    
    if prev_sales > 0:
        pct_change = ((current_sales - prev_sales) / prev_sales) * 100
    else:
        pct_change = 0
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Current Period Sales",
            f"${current_sales:,.2f}",
            f"{pct_change:+.1f}% vs previous period" if prev_sales > 0 else "No prior data"
        )
    
    with col2:
        st.metric(
            "Previous Period Sales",
            f"${prev_sales:,.2f}"
        )
    
    with col3:
        st.metric(
            "Period Length",
            f"{days_diff} days"
        )
    
    if 'locations' in filters and len(filters['locations']) > 1:
        st.write("**Sales by Location:**")
        location_sales = df_sales.groupby('location_name')['total'].sum().reset_index()
        location_sales.columns = ['Location', 'Sales']
        location_sales['Sales'] = location_sales['Sales'].apply(lambda x: f"${x:,.2f}")
        
        cols = st.columns(len(location_sales))
        for idx, (_, row) in enumerate(location_sales.iterrows()):
            with cols[idx]:
                st.metric(row['Location'], row['Sales'])
    
    daily_sales = df_sales.groupby('date')['total'].sum().reset_index()
    fig = px.line(
        daily_sales,
        x='date',
        y='total',
        title='Daily Sales Trend',
        labels={'total': 'Sales ($)', 'date': 'Date'}
    )
    st.plotly_chart(fig, use_container_width=True)


def render_basket_economics(df_orders):
    """Render basket economics section"""
    df_sales = df_orders[~df_orders['voided']].copy()
    
    if 'line_items_for_basket' in st.session_state:
        df_line_items = st.session_state['line_items_for_basket']
        filtered_order_ids = df_sales['order_id'].unique()
        df_line_items_filtered = df_line_items[df_line_items['order_id'].isin(filtered_order_ids)]
    else:
        df_line_items_filtered = None
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_orders = len(df_sales)
        total_sales = df_sales['total'].sum()
        aov = total_sales / total_orders if total_orders > 0 else 0
        st.metric(
            "Average Order Value (AOV)", 
            f"${aov:.2f}",
            help="Average revenue per transaction. Formula: total_sales / number_of_orders"
        )
    
    with col2:
        if df_line_items_filtered is not None and len(df_line_items_filtered) > 0:
            total_items = df_line_items_filtered['quantity'].sum()
            items_per_ticket = total_items / total_orders if total_orders > 0 else 0
            st.metric(
                "Items per Ticket", 
                f"{items_per_ticket:.1f}",
                help="Average number of items (units) per order. Formula: Σ(quantity) / number_of_orders"
            )
        else:
            st.metric("Items per Ticket", "N/A")
    
    with col3:
        st.metric(
            "Total Orders", 
            f"{total_orders:,}",
            help="Number of completed transactions (excluding voids)"
        )
    
    col1, col2 = st.columns(2)
    
    with col1:
        tender_mix = df_sales.groupby('tender_type')['total'].sum().reset_index()
        tender_mix['percentage'] = tender_mix['total'] / tender_mix['total'].sum() * 100
        
        fig = px.pie(
            tender_mix,
            values='total',
            names='tender_type',
            title='Tender Mix'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        order_type_dist = df_sales.groupby('order_type')['total'].sum().reset_index()
        
        fig = px.bar(
            order_type_dist,
            x='order_type',
            y='total',
            title='Sales by Order Type',
            labels={'total': 'Sales ($)', 'order_type': 'Order Type'}
        )
        st.plotly_chart(fig, use_container_width=True)


def render_discount_analysis(df_orders):
    """Render discount and promo analysis"""
    df_sales = df_orders[~df_orders['voided']].copy()
    
    with_discount = df_sales[df_sales['discount'] > 0]
    without_discount = df_sales[df_sales['discount'] == 0]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        avg_with = with_discount['total'].mean() if len(with_discount) > 0 else 0
        st.metric(
            "AOV with Promo", 
            f"${avg_with:.2f}",
            help="Average order value for transactions with discounts applied. Formula: Σ(total where discount > 0) / COUNT(orders with discount)"
        )
    
    with col2:
        avg_without = without_discount['total'].mean() if len(without_discount) > 0 else 0
        st.metric(
            "AOV without Promo", 
            f"${avg_without:.2f}",
            help="Average order value for transactions without discounts. Formula: Σ(total where discount = 0) / COUNT(orders without discount)"
        )
    
    with col3:
        total_discount = df_sales['discount'].sum()
        discount_rate = (total_discount / (df_sales['total'].sum() + total_discount) * 100) if df_sales['total'].sum() > 0 else 0
        st.metric(
            "Total Discounts", 
            f"${total_discount:,.2f}",
            help=f"Total discount amount given (≈{discount_rate:.1f}% of pre-discount sales). Formula: Σ(discount)"
        )
    
    if 'promo_code' in df_sales.columns:
        top_promos = df_sales[df_sales['promo_code'].notna()].groupby('promo_code').agg({
            'order_id': 'count',
            'discount': 'sum',
            'total': 'sum'
        }).reset_index()
        top_promos.columns = ['Promo Code', 'Orders', 'Total Discount', 'Sales']
        top_promos = top_promos.sort_values('Total Discount', ascending=False).head(5)
        
        st.write("**Top Active Promos:**")
        st.dataframe(top_promos, use_container_width=True, hide_index=True)


def render_exceptions(df_orders, exceptions_df):
    """Render voids/refunds exceptions"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        void_rate = (df_orders['voided'].sum() / len(df_orders) * 100) if len(df_orders) > 0 else 0
        st.metric(
            "Overall Void Rate", 
            f"{void_rate:.2f}%",
            help="Percentage of transactions that were voided. Formula: COUNT(voided=true) / COUNT(all_orders) × 100. Spikes >2× median indicate training needs."
        )
    
    with col2:
        refund_rate = (df_orders['refunded'].sum() / len(df_orders) * 100) if len(df_orders) > 0 else 0
        st.metric(
            "Overall Refund Rate", 
            f"{refund_rate:.2f}%",
            help="Percentage of transactions that were refunded. Formula: COUNT(refunded=true) / COUNT(all_orders) × 100. Monitor for orphan refunds (no linked sale)."
        )
    
    with col3:
        daily_voids = df_orders.groupby('date')['voided'].sum()
        median_voids = daily_voids.median() if len(daily_voids) > 0 else 0
        st.metric(
            "Daily Median Voids", 
            f"{median_voids:.0f}",
            help="Median number of voids per day. Used as baseline to flag spike days (>2× this value) for coaching opportunities."
        )
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Void Rate by Budtender (Top 10)**")
        staff_exceptions = df_orders.groupby('staff_id').agg({
            'voided': 'sum',
            'refunded': 'sum',
            'order_id': 'count'
        }).reset_index()
        staff_exceptions['void_rate'] = staff_exceptions['voided'] / staff_exceptions['order_id'] * 100
        staff_exceptions = staff_exceptions.sort_values('void_rate', ascending=False).head(10)
        
        fig = px.bar(
            staff_exceptions,
            x='staff_id',
            y='void_rate',
            title='',
            labels={'void_rate': 'Void Rate (%)', 'staff_id': 'Cashier'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.write("**Void/Refund Rate by Hour**")
        hourly_exceptions = df_orders.groupby('hour').agg({
            'voided': 'sum',
            'refunded': 'sum',
            'order_id': 'count'
        }).reset_index()
        hourly_exceptions['exception_rate'] = (hourly_exceptions['voided'] + hourly_exceptions['refunded']) / hourly_exceptions['order_id'] * 100
        
        fig = px.line(
            hourly_exceptions,
            x='hour',
            y='exception_rate',
            title='',
            labels={'exception_rate': 'Exception Rate (%)', 'hour': 'Hour of Day'},
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)


def render_top_movers(df_line_items, df_products):
    """Render top/bottom movers"""
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Top 10 SKUs by Net Sales**")
        top_by_sales = df_line_items.groupby(['product_name', 'category']).agg({
            'total': 'sum',
            'quantity': 'sum'
        }).reset_index()
        top_by_sales = top_by_sales.sort_values('total', ascending=False).head(10)
        top_by_sales.columns = ['Product', 'Category', 'Sales ($)', 'Units']
        st.dataframe(top_by_sales, use_container_width=True, hide_index=True)
    
    with col2:
        st.write("**Top 10 SKUs by Margin $**")
        top_by_margin = df_line_items.groupby(['product_name', 'category']).agg({
            'margin': 'sum',
            'quantity': 'sum'
        }).reset_index()
        top_by_margin = top_by_margin.sort_values('margin', ascending=False).head(10)
        top_by_margin.columns = ['Product', 'Category', 'Margin ($)', 'Units']
        st.dataframe(top_by_margin, use_container_width=True, hide_index=True)
    
    st.write("**Category Contribution (Pareto 80/20)**")
    category_sales = df_line_items.groupby('category')['total'].sum().reset_index()
    category_sales = category_sales.sort_values('total', ascending=False)
    category_sales['cumulative_pct'] = category_sales['total'].cumsum() / category_sales['total'].sum() * 100
    category_sales['sales_pct'] = category_sales['total'] / category_sales['total'].sum() * 100
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=category_sales['category'],
        y=category_sales['sales_pct'],
        name='Sales %',
        yaxis='y',
        text=category_sales['sales_pct'].apply(lambda x: f'{x:.1f}%'),
        textposition='auto'
    ))
    fig.add_trace(go.Scatter(
        x=category_sales['category'],
        y=category_sales['cumulative_pct'],
        name='Cumulative %',
        yaxis='y2',
        mode='lines+markers',
        line=dict(color='red', width=2)
    ))
    fig.update_layout(
        title='',
        yaxis=dict(title='Sales %', range=[0, 100]),
        yaxis2=dict(title='Cumulative %', overlaying='y', side='right', range=[0, 100]),
        hovermode='x',
        height=400
    )
    fig.add_hline(y=80, line_dash="dash", line_color="green", annotation_text="80%", yref='y2')
    st.plotly_chart(fig, use_container_width=True)


def render_compliance_panel(df_orders, exceptions_df):
    """Render compliance-friendly panel"""
    df_sales = df_orders[~df_orders['voided']].copy()
    
    col1, col2 = st.columns(2)
    
    with col1:
        medical_sales = df_sales[df_sales['is_medical']]['total'].sum()
        adult_sales = df_sales[~df_sales['is_medical']]['total'].sum()
        
        mix_df = pd.DataFrame({
            'Type': ['Adult-Use', 'Medical'],
            'Sales': [adult_sales, medical_sales]
        })
        
        fig = px.pie(mix_df, values='Sales', names='Type', title='Adult-Use vs Medical Mix')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        tax_breakdown = pd.DataFrame({
            'Tax Type': ['Excise', 'State', 'Local'],
            'Amount': [
                df_sales['excise_tax'].sum(),
                df_sales['state_tax'].sum(),
                df_sales['local_tax'].sum()
            ]
        })
        
        fig = px.bar(tax_breakdown, x='Tax Type', y='Amount', title='Tax Breakdown')
        st.plotly_chart(fig, use_container_width=True)
    
    if len(exceptions_df) > 0:
        st.write("**Issues Detected:**")
        
        for _, exc in exceptions_df.head(10).iterrows():
            if exc['type'] in ['negative_total', 'tax_mismatch']:
                st.markdown(f"<div class='exception-error'>❌ {exc['description']}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='exception-warning'>⚠️ {exc['description']}</div>", unsafe_allow_html=True)
    else:
        st.success("✅ No compliance issues detected")


def render_heatmap(df_orders):
    """Render hourly heatmap"""
    hourly_data = df_orders.groupby(['date', 'hour']).agg({
        'order_id': 'count',
        'voided': 'sum',
        'discount': lambda x: (x > 0).sum()
    }).reset_index()
    hourly_data.columns = ['date', 'hour', 'transactions', 'voids', 'discounts']
    
    tab1, tab2, tab3 = st.tabs(["Transaction Throughput", "Void Activity", "Discount Activity"])
    
    with tab1:
        st.write("**Transaction volume by hour to identify peak times**")
        heatmap_data = hourly_data.pivot(index='date', columns='hour', values='transactions').fillna(0)
        fig = px.imshow(
            heatmap_data,
            labels=dict(x="Hour", y="Date", color="Transactions"),
            title="",
            aspect="auto",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.write("**Void activity by hour to spot coaching windows**")
        heatmap_data = hourly_data.pivot(index='date', columns='hour', values='voids').fillna(0)
        fig = px.imshow(
            heatmap_data,
            labels=dict(x="Hour", y="Date", color="Voids"),
            title="",
            aspect="auto",
            color_continuous_scale="Reds"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.write("**Discount activity by hour**")
        heatmap_data = hourly_data.pivot(index='date', columns='hour', values='discounts').fillna(0)
        fig = px.imshow(
            heatmap_data,
            labels=dict(x="Hour", y="Date", color="Discounts"),
            title="",
            aspect="auto",
            color_continuous_scale="Greens"
        )
        st.plotly_chart(fig, use_container_width=True)


def render_notes_section():
    """Render notes for GM"""
    st.subheader("Notes for Today")
    notes = st.text_area(
        "Manager Action Items",
        placeholder="Enter notes, observations, or action items here...",
        height=100
    )
    
    if st.button("Save Notes"):
        st.success("✅ Notes saved!")


def render_upload_interface():
    """Render file upload interface when no data is loaded"""
    
    st.markdown("""
    <style>
        .upload-container {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 3rem;
            border-radius: 20px;
            text-align: center;
            margin: 2rem 0;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }
        .upload-icon {
            font-size: 5rem;
            margin-bottom: 1rem;
            animation: float 3s ease-in-out infinite;
        }
        @keyframes float {
            0%, 100% { transform: translateY(0px); }
            50% { transform: translateY(-20px); }
        }
        .upload-title {
            font-size: 2.5rem;
            font-weight: 700;
            color: white;
            margin-bottom: 0.5rem;
        }
        .upload-subtitle {
            font-size: 1.2rem;
            color: rgba(255,255,255,0.9);
            margin-bottom: 2rem;
        }
        .feature-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin: 2rem 0;
        }
        .feature-card {
            background: white;
            padding: 1.5rem;
            border-radius: 15px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        .feature-card:hover {
            transform: translateY(-5px);
        }
        .feature-icon {
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
        }
        .feature-title {
            font-size: 1.1rem;
            font-weight: 600;
            color: #333;
            margin-bottom: 0.5rem;
        }
        .feature-desc {
            font-size: 0.9rem;
            color: #666;
        }
    </style>
    
    <div class='upload-container'>
        <div class='upload-icon'>📊</div>
        <div class='upload-title'>Welcome to Dutchie POS Dashboard</div>
        <div class='upload-subtitle'>Upload your transaction CSV files to get started</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class='feature-grid'>
        <div class='feature-card'>
            <div class='feature-icon'>⚡</div>
            <div class='feature-title'>Real-time Analytics</div>
            <div class='feature-desc'>Instant KPI calculations and insights</div>
        </div>
        <div class='feature-card'>
            <div class='feature-icon'>📈</div>
            <div class='feature-title'>Trend Analysis</div>
            <div class='feature-desc'>Sales patterns and forecasting</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("### 📁 Upload Your Data")
        st.markdown("Drag and drop your CSV transaction files or click to browse")
        
        uploaded_files = st.file_uploader(
            "Choose CSV files",
            type=['csv'],
            accept_multiple_files=True,
            help="Upload transaction files (e.g., Columbus_transactions.csv, Cincinnati_transactions.csv)",
            label_visibility="collapsed"
        )
        
        if uploaded_files:
            st.success(f"✅ {len(uploaded_files)} file(s) selected")
            
            for uploaded_file in uploaded_files:
                st.write(f"📄 {uploaded_file.name}")
            
            if st.button("🚀 Load Data", type="primary", use_container_width=True):
                with st.spinner("Processing files and loading into database..."):
                    try:
                        all_data = {'orders': [], 'line_items': [], 'products': [], 'staff': []}
                        
                        for uploaded_file in uploaded_files:
                            location_name = uploaded_file.name.replace('_transactions.csv', '').replace('.csv', '').replace('_', ' ')
                            
                            uploaded_data = parse_uploaded_file(uploaded_file, location_name)
                            is_valid, message = validate_uploaded_data(uploaded_data)
                            
                            if not is_valid:
                                st.error(f"❌ {uploaded_file.name}: {message}")
                                return
                            
                            all_data['orders'].extend(uploaded_data['orders'])
                            all_data['line_items'].extend(uploaded_data['line_items'])
                            all_data['products'].extend(uploaded_data['products'])
                            all_data['staff'].extend(uploaded_data['staff'])
                        
                        cleaned_data = clean_data(all_data)
                        exceptions = detect_exceptions(cleaned_data['orders'])
                        
                        db = DutchieDB()
                        db.connect()
                        db.create_schema()
                        db.load_data(cleaned_data)
                        db.close()
                        
                        st.session_state['cleaned_data'] = cleaned_data
                        st.session_state['exceptions'] = exceptions
                        st.session_state['data_loaded'] = True
                        
                        st.success("✅ Data loaded successfully!")
                        time.sleep(1)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Error loading data: {str(e)}")
        else:
            st.info("💡 Tip: You can upload multiple location files at once for comparative analysis")


def main():
    """Main dashboard application"""
    
    start_time = time.time()
    
    st.markdown("<div class='main-header'>Dutchie POS Manager Dashboard</div>", unsafe_allow_html=True)
    
    if 'data_loaded' not in st.session_state:
        st.session_state['data_loaded'] = False
    
    if not st.session_state['data_loaded']:
        render_upload_interface()
        return
    
    if st.sidebar.button("🔄 Load New Data"):
        st.session_state['data_loaded'] = False
        if 'cleaned_data' in st.session_state:
            del st.session_state['cleaned_data']
        if 'exceptions' in st.session_state:
            del st.session_state['exceptions']
        st.rerun()
    
    cleaned_data = st.session_state['cleaned_data']
    exceptions = st.session_state['exceptions']
    
    df_orders = cleaned_data['orders']
    df_line_items = cleaned_data['line_items']
    df_products = cleaned_data['products']
    df_staff = cleaned_data['staff']
    
    st.session_state['all_orders'] = df_orders.copy()
    st.session_state['line_items_for_basket'] = df_line_items.copy()
    
    if len(df_orders) == 0:
        st.markdown("""
        <div class='empty-state'>
            <div class='empty-state-icon'>📂</div>
            <div class='empty-state-text'>No data loaded</div>
            <p>Upload a CSV file using the sidebar to get started, or check your date range filters.</p>
        </div>
        """, unsafe_allow_html=True)
        st.stop()
    
    filters = render_filters(df_orders, df_line_items)
    
    df_orders_filtered = apply_filters(df_orders, filters, is_line_items=False)
    
    if len(df_orders_filtered) == 0:
        st.markdown("""
        <div class='empty-state'>
            <div class='empty-state-icon'>🔍</div>
            <div class='empty-state-text'>No data matches your filters</div>
            <p>Try adjusting your date range, location, or other filter selections.</p>
        </div>
        """, unsafe_allow_html=True)
        st.stop()
    
    filtered_order_ids = df_orders_filtered['order_id'].unique()
    df_line_items_filtered = df_line_items[df_line_items['order_id'].isin(filtered_order_ids)]
    
    df_line_items_filtered = apply_filters(df_line_items_filtered, filters, is_line_items=True)
    
    if 'category' in filters:
        category_order_ids = df_line_items_filtered['order_id'].unique()
        df_orders_filtered = df_orders_filtered[df_orders_filtered['order_id'].isin(category_order_ids)]
    
    render_kpi_cards(df_orders_filtered, df_line_items_filtered)
    
    st.divider()
    
    st.header("1. Net Sales & Same-Store")
    render_sales_comparison(df_orders_filtered, filters)
    
    st.divider()
    
    st.header("2. Basket Economics")
    render_basket_economics(df_orders_filtered)
    
    st.divider()
    
    st.header("3. Discount/Promo Impact")
    render_discount_analysis(df_orders_filtered)
    
    st.divider()
    
    st.header("4. Voids/Refunds Exception Monitor")
    render_exceptions(df_orders_filtered, exceptions)
    
    st.divider()
    
    # 5. TOP/BOTTOM MOVERS
    st.header("5. Top/Bottom Movers")
    render_top_movers(df_line_items_filtered, df_products)
    
    st.divider()
    
    # 6. COMPLIANCE-FRIENDLY PANEL
    st.header("6. Compliance-Friendly Panel")
    render_compliance_panel(df_orders_filtered, exceptions)
    
    st.divider()
    
    st.header("7. Heatmap by Hour")
    render_heatmap(df_orders_filtered)
    
    st.divider()
    
    render_notes_section()
    
    if hasattr(st.session_state, 'debug_mode') and st.session_state.debug_mode:
        render_time = time.time() - start_time
        if render_time > 2.0:
            st.caption(f"Page rendered in {render_time:.2f}s (target: <2s)")
        else:
            st.caption(f"Page rendered in {render_time:.2f}s")


if __name__ == "__main__":
    main()
