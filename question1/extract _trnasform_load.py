import pandas as pd
import sqlite3

# Step 1: Extract Data
def extract_data(file_path_a, file_path_b):
    # Read CSV files
    df_a = pd.read_csv(file_path_a)
    df_b = pd.read_csv(file_path_b)
    return df_a, df_b

# Step 2: Transform Data
def transform_data(df_a, df_b):
    # Add region column
    df_a['region'] = 'A'
    df_b['region'] = 'B'

    # Combine data
    combined_df = pd.concat([df_a, df_b], ignore_index=True)

    # Calculate total_sales
    combined_df['total_sales'] = combined_df['QuantityOrdered'] * combined_df['ItemPrice']

    # Calculate net_sale
    combined_df['net_sale'] = combined_df['total_sales'] - combined_df['PromotionDiscount']

    # Remove duplicates based on OrderId
    combined_df.drop_duplicates(subset=['OrderId'], keep='first', inplace=True)

    # Filter out orders with negative or zero net_sale
    combined_df = combined_df[combined_df['net_sale'] > 0]

    return combined_df

# Step 3: Load Data
def load_data(df, db_path, table_name):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            OrderId INTEGER PRIMARY KEY,
            OrderItemId INTEGER,
            QuantityOrdered INTEGER,
            ItemPrice REAL,
            PromotionDiscount REAL,
            total_sales REAL,
            net_sale REAL,
            region TEXT
        )
    ''')

    # Insert data into the table
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Commit and close connection
    conn.commit()
    conn.close()

# Step 4: Validate Data
def validate_data(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query 1: Count total records
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_records = cursor.fetchone()[0]
    print(f"Total records: {total_records}")

    # Query 2: Total sales by region
    cursor.execute(f"SELECT region, SUM(total_sales) FROM {table_name} GROUP BY region")
    total_sales_by_region = cursor.fetchall()
    print("Total sales by region:", total_sales_by_region)

    # Query 3: Average sales per transaction
    cursor.execute(f"SELECT AVG(total_sales) FROM {table_name}")
    avg_sales = cursor.fetchone()[0]
    print(f"Average sales per transaction: {avg_sales}")

    # Query 4: Check for duplicate OrderIds
    cursor.execute(f"SELECT OrderId, COUNT(*) FROM {table_name} GROUP BY OrderId HAVING COUNT(*) > 1")
    duplicates = cursor.fetchall()
    print("Duplicate OrderIds:", duplicates)

    conn.close()

# Main function
def main():
    # File paths
    file_path_a = 'data/order_region_a.csv'
    file_path_b = 'data/order_region_b.csv'

    # Extract data
    df_a, df_b = extract_data(file_path_a, file_path_b)

    # Transform data
    transformed_df = transform_data(df_a, df_b)

    # Load data
    db_path = 'sales_data.db'
    table_name = 'sales_data'
    load_data(transformed_df, db_path, table_name)

    # Validate data
    validate_data(db_path, table_name)

if __name__ == "__main__":
    main()