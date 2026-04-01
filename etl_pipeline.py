import pandas as pd
from sqlalchemy import create_engine
import os

def extract(engine):
    print("--- 1. Extracting Data ---")
    tables = ['customers', 'products', 'orders', 'order_items']
    data_dict = {}
    for table in tables:
        data_dict[table] = pd.read_sql(f"SELECT * FROM {table}", engine)
        print(f"✅ Extracted {len(data_dict[table])} rows from {table}")
    return data_dict

def transform(data_dict):
    print("--- 2. Transforming Data ---")
    # سحب الجداول من الديكشنري
    df_customers = data_dict['customers']
    df_products = data_dict['products']
    df_orders = data_dict['orders']
    df_items = data_dict['order_items']

    # 1. دمج الطلبات مع بنود الطلبات
    df = df_orders.merge(df_items, on='order_id')
    
    # 2. دمج المنتجات (تغيير اسم العمود name ليكون product_name)
    df = df.merge(df_products.rename(columns={'name': 'product_name'}), on='product_id')
    
    # 3. دمج الزبائن (تغيير اسم العمود name ليكون customer_name)
    df = df.merge(df_customers.rename(columns={'name': 'customer_name'}), on='customer_id')

    # 4. حساب الإجمالي لكل بند
    df['line_total'] = df['quantity'] * df['unit_price']

    # 5. تنظيف البيانات (استبعاد الملغي والكميات الكبيرة جداً)
    df = df[df['status'] != 'cancelled']
    df = df[df['quantity'] <= 100]

    # 6. التجميع على مستوى الزبون
    customer_summary = df.groupby(['customer_id', 'customer_name']).agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()

    # 7. حساب متوسط قيمة الطلب
    customer_summary['avg_order_value'] = customer_summary['total_revenue'] / customer_summary['total_orders']

    print(f"✅ Transformation complete. Summary for {len(customer_summary)} customers.")
    return customer_summary

def validate(df):
    print("--- 3. Validating Data ---")
    checks = {
        "No Null IDs": df['customer_id'].notnull().all(),
        "Positive Revenue": (df['total_revenue'] > 0).all(),
        "Unique Customers": df['customer_id'].is_unique,
        "Orders > 0": (df['total_orders'] > 0).all()
    }

    for check, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"Check '{check}': {status}")
        if not passed:
            raise ValueError(f"CRITICAL ERROR: {check} failed!")
    return True

def load(df, engine, csv_path):
    print("--- 4. Loading Data ---")
    # التأكد من وجود مجلد output
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # حفظ في قاعدة البيانات وفي ملف CSV
    df.to_sql('customer_analytics', engine, if_exists='replace', index=False)
    df.to_csv(csv_path, index=False)
    print(f"✅ Row count loaded: {len(df)}")
    print(f"✅ Data saved to DB table 'customer_analytics' and {csv_path}")

if __name__ == "__main__":
    # إعداد محرك قاعدة البيانات
    db_url = "postgresql://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(db_url)
    
    try:
        # تشغيل الـ Pipeline بالترتيب
        data = extract(engine)
        clean_data = transform(data)
        validate(clean_data)
        load(clean_data, engine, "output/customer_analytics.csv")
        print("\n🚀 🚀 ETL Pipeline Finished Successfully! 🚀 🚀")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")