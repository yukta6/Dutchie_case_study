"""
Database module - DuckDB for local analytics
"""
import duckdb
import pandas as pd
from config import DB_PATH


class DutchieDB:
    """Database manager for Dutchie POS data"""
    
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """Connect to DuckDB database"""
        self.conn = duckdb.connect(self.db_path)
        return self.conn
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def create_schema(self):
        """Create star schema tables"""
        if not self.conn:
            self.connect()
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS DimLocation (
                location_id VARCHAR PRIMARY KEY,
                location_name VARCHAR,
                timezone VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS DimStaff (
                staff_id VARCHAR PRIMARY KEY,
                staff_name VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS DimProduct (
                product_id VARCHAR PRIMARY KEY,
                product_name VARCHAR,
                category VARCHAR,
                subcategory VARCHAR,
                unit_cost DECIMAL(10,2),
                unit_price DECIMAL(10,2)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS DimTime (
                time_id VARCHAR PRIMARY KEY,
                timestamp TIMESTAMP,
                date DATE,
                hour INTEGER,
                daypart VARCHAR,
                day_of_week VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS FactSales (
                order_id VARCHAR PRIMARY KEY,
                location_id VARCHAR,
                staff_id VARCHAR,
                time_id VARCHAR,
                order_type VARCHAR,
                is_medical BOOLEAN,
                subtotal DECIMAL(10,2),
                excise_tax DECIMAL(10,2),
                state_tax DECIMAL(10,2),
                local_tax DECIMAL(10,2),
                total_tax DECIMAL(10,2),
                discount DECIMAL(10,2),
                discount_rate DECIMAL(5,2),
                total DECIMAL(10,2),
                tender_type VARCHAR,
                voided BOOLEAN,
                refunded BOOLEAN,
                promo_code VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS FactLineItems (
                line_id VARCHAR PRIMARY KEY,
                order_id VARCHAR,
                product_id VARCHAR,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                unit_cost DECIMAL(10,2),
                discount DECIMAL(10,2),
                total DECIMAL(10,2),
                margin DECIMAL(10,2)
            )
        """)
    
    def load_data(self, cleaned_data):
        """Load cleaned data into star schema"""
        if not self.conn:
            self.connect()
        
        self.conn.execute("DELETE FROM FactLineItems")
        self.conn.execute("DELETE FROM FactSales")
        self.conn.execute("DELETE FROM DimTime")
        self.conn.execute("DELETE FROM DimProduct")
        self.conn.execute("DELETE FROM DimStaff")
        self.conn.execute("DELETE FROM DimLocation")
        
        df_products = cleaned_data['products'].drop_duplicates(subset=['product_id'])
        self.conn.execute("""
            INSERT INTO DimProduct 
            SELECT product_id, name as product_name, category, subcategory, unit_cost, unit_price
            FROM df_products
        """)
        
        df_staff = cleaned_data['staff'].drop_duplicates(subset=['staff_id'])
        self.conn.execute("""
            INSERT INTO DimStaff 
            SELECT staff_id, name as staff_name
            FROM df_staff
        """)
        
        df_orders = cleaned_data['orders']
        locations_df = df_orders[['location_id', 'location_name']].drop_duplicates(subset=['location_id'])
        locations_df['timezone'] = locations_df['location_name'].map(
            lambda x: 'America/New_York'
        )
        self.conn.execute("""
            INSERT INTO DimLocation 
            SELECT * FROM locations_df
        """)
        
        time_df = df_orders[['time_id', 'timestamp_local', 'date', 'hour', 'daypart', 'day_of_week']].drop_duplicates(subset=['time_id'])
        time_df = time_df.rename(columns={'timestamp_local': 'timestamp'})
        self.conn.execute("""
            INSERT INTO DimTime 
            SELECT * FROM time_df
        """)
        
        sales_df = df_orders[[
            'order_id', 'location_id', 'staff_id', 'time_id', 'order_type', 'is_medical',
            'subtotal', 'excise_tax', 'state_tax', 'local_tax', 'total_tax', 
            'discount', 'discount_rate', 'total', 'tender_type', 'voided', 'refunded', 'promo_code'
        ]].drop_duplicates(subset=['order_id'])
        self.conn.execute("""
            INSERT INTO FactSales 
            SELECT * FROM sales_df
        """)
        
        df_line_items = cleaned_data['line_items']
        line_items_df = df_line_items[[
            'line_id', 'order_id', 'product_id', 'quantity', 
            'unit_price', 'unit_cost', 'discount', 'total', 'margin'
        ]].drop_duplicates(subset=['line_id'])
        self.conn.execute("""
            INSERT INTO FactLineItems 
            SELECT * FROM line_items_df
        """)
    
    def query(self, sql):
        """Execute SQL query and return DataFrame"""
        if not self.conn:
            self.connect()
        return self.conn.execute(sql).df()
    
    def get_kpis(self, filters=None):
        """Get KPIs with optional filters"""
        where_clauses = self._build_where_clause(filters)
        
        kpis = {}
        
        sql = f"""
            SELECT 
                SUM(CASE WHEN NOT voided THEN total ELSE 0 END) as net_sales,
                COUNT(DISTINCT order_id) as total_orders,
                AVG(CASE WHEN NOT voided THEN total ELSE NULL END) as aov,
                SUM(CASE WHEN voided THEN 1 ELSE 0 END) as void_count,
                SUM(CASE WHEN refunded THEN 1 ELSE 0 END) as refund_count
            FROM FactSales fs
            JOIN DimTime dt ON fs.time_id = dt.time_id
            {where_clauses}
        """
        kpis['sales'] = self.query(sql)
        
        sql = f"""
            SELECT 
                tender_type,
                SUM(CASE WHEN NOT voided THEN total ELSE 0 END) as sales,
                COUNT(*) as transactions
            FROM FactSales fs
            JOIN DimTime dt ON fs.time_id = dt.time_id
            {where_clauses}
            GROUP BY tender_type
            ORDER BY sales DESC
        """
        kpis['tender_mix'] = self.query(sql)
        
        sql = f"""
            SELECT 
                dp.product_name,
                dp.category,
                SUM(fli.quantity) as units_sold,
                SUM(fli.total) as net_sales,
                SUM(fli.margin) as total_margin
            FROM FactLineItems fli
            JOIN DimProduct dp ON fli.product_id = dp.product_id
            JOIN FactSales fs ON fli.order_id = fs.order_id
            JOIN DimTime dt ON fs.time_id = dt.time_id
            {where_clauses.replace('WHERE', 'WHERE NOT fs.voided AND')}
            GROUP BY dp.product_name, dp.category
            ORDER BY net_sales DESC
            LIMIT 10
        """
        kpis['top_products'] = self.query(sql)
        
        sql = f"""
            SELECT 
                dp.category,
                SUM(fli.total) as net_sales,
                SUM(fli.margin) as total_margin
            FROM FactLineItems fli
            JOIN DimProduct dp ON fli.product_id = dp.product_id
            JOIN FactSales fs ON fli.order_id = fs.order_id
            JOIN DimTime dt ON fs.time_id = dt.time_id
            {where_clauses.replace('WHERE', 'WHERE NOT fs.voided AND')}
            GROUP BY dp.category
            ORDER BY net_sales DESC
        """
        kpis['category_mix'] = self.query(sql)
        
        sql = f"""
            SELECT 
                dt.hour,
                COUNT(*) as transactions,
                SUM(CASE WHEN fs.voided THEN 1 ELSE 0 END) as voids,
                SUM(CASE WHEN fs.discount > 0 THEN 1 ELSE 0 END) as discounted
            FROM FactSales fs
            JOIN DimTime dt ON fs.time_id = dt.time_id
            {where_clauses}
            GROUP BY dt.hour
            ORDER BY dt.hour
        """
        kpis['hourly'] = self.query(sql)
        
        return kpis
    
    def _build_where_clause(self, filters):
        """Build WHERE clause from filters"""
        if not filters:
            return ""
        
        clauses = []
        
        if filters.get('start_date') and filters.get('end_date'):
            clauses.append(f"dt.date BETWEEN '{filters['start_date']}' AND '{filters['end_date']}'")
        
        if filters.get('locations'):
            locations = "','".join(filters['locations'])
            clauses.append(f"fs.location_id IN ('{locations}')")
        
        if filters.get('order_type'):
            clauses.append(f"fs.order_type = '{filters['order_type']}'")
        
        if filters.get('daypart'):
            clauses.append(f"dt.daypart = '{filters['daypart']}'")
        
        if filters.get('category'):
            clauses.append(f"EXISTS (SELECT 1 FROM FactLineItems fli JOIN DimProduct dp ON fli.product_id = dp.product_id WHERE fli.order_id = fs.order_id AND dp.category = '{filters['category']}')")
        
        if filters.get('staff_id'):
            clauses.append(f"fs.staff_id = '{filters['staff_id']}'")
        
        if clauses:
            return "WHERE " + " AND ".join(clauses)
        return ""
