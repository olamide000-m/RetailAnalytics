import pandas as pd
import pyodbc
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(filename='etl_pipeline.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# SQL Server connection string
conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=DESKTOP-JPU0ABR\\SQLEXPRESS;"
    "DATABASE=RetailAnalytics;"
    "Trusted_Connection=yes;"
)

def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Extracted {len(df)} records from {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error extracting data from {file_path}: {str(e)}")
        raise

def transform_data(df, data_type):
    try:
        if data_type == "transactions":
            df['Date'] = pd.to_datetime(df['Date'])
            df['Amount'] = df['Amount'].fillna(0)
            df['Season'] = df['Date'].dt.month.map({
                12: 'Winter', 1: 'Winter', 2: 'Winter',
                3: 'Spring', 4: 'Spring', 5: 'Spring',
                6: 'Summer', 7: 'Summer', 8: 'Summer',
                9: 'Fall', 10: 'Fall', 11: 'Fall'
            })
        elif data_type == "engagements":
            df['Date'] = pd.to_datetime(df['Date'])
            df['Duration'] = df['Duration'].fillna(0)
        logging.info(f"Transformed {len(df)} {data_type} records")
        return df
    except Exception as e:
        logging.error(f"Error transforming {data_type} data: {str(e)}")
        raise

def load_data(df, table_name):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        for _, row in df.iterrows():
            if table_name == "Transactions":
                cursor.execute("""
                    INSERT INTO Transactions (CustomerID, StoreID, Date, Amount, ProductCategory)
                    VALUES (?, ?, ?, ?, ?)
                """, row['CustomerID'], row['StoreID'], row['Date'], row['Amount'], row['ProductCategory'])
            elif table_name == "Engagements":
                cursor.execute("""
                    INSERT INTO Engagements (CustomerID, InteractionType, Date, Duration)
                    VALUES (?, ?, ?, ?)
                """, row['CustomerID'], row['InteractionType'], row['Date'], row['Duration'])
        conn.commit()
        logging.info(f"Loaded {len(df)} records into {table_name}")
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {str(e)}")
        raise

def main():
    try:
        # Extract
        transactions_df = extract_data("transactions.csv")
        engagements_df = extract_data("engagements.csv")
        
        # Transform
        transactions_df = transform_data(transactions_df, "transactions")
        engagements_df = transform_data(engagements_df, "engagements")
        
        # Load
        load_data(transactions_df, "Transactions")
        load_data(engagements_df, "Engagements")
        
        logging.info("ETL pipeline completed successfully")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()