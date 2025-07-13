import pandas as pd
import pyodbc

# SQL Server connection
conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=DESKTOP-JPU0ABR\\SQLEXPRESS;"
    "DATABASE=RetailAnalytics;"
    "Trusted_Connection=yes;"
)

def fetch_data(query):
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def segment_customers():
    query = """
    SELECT 
        c.CustomerID,
        c.Name,
        SUM(t.Amount) AS TotalSpend,
        COUNT(t.TransactionID) AS PurchaseCount
    FROM Transactions t
    JOIN Customers c ON t.CustomerID = c.CustomerID
    GROUP BY c.CustomerID, c.Name
    """
    df = fetch_data(query)
    
    # Segment customers
    df['Segment'] = pd.qcut(df['TotalSpend'], q=3, labels=['Low', 'Medium', 'High'])
    
    # Save to CSV for Power BI
    df.to_csv('customer_segments.csv', index=False)
    print("Customer segments saved to customer_segments.csv")

if __name__ == "__main__":
    segment_customers()