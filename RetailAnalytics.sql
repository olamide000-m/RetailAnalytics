-- Create Customers table
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    Name NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100) UNIQUE NOT NULL,
    Region NVARCHAR(50),
    JoinDate DATE
);

-- Create Stores table
CREATE TABLE Stores (
    StoreID INT PRIMARY KEY IDENTITY(1,1),
    Location NVARCHAR(100) NOT NULL,
    Region NVARCHAR(50)
);

-- Create Transactions table
CREATE TABLE Transactions (
    TransactionID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT,
    StoreID INT,
    Date DATE NOT NULL,
    Amount DECIMAL(10,2) NOT NULL,
    ProductCategory NVARCHAR(50),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (StoreID) REFERENCES Stores(StoreID)
);

-- Create Engagements table
CREATE TABLE Engagements (
    EngagementID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT,
    InteractionType NVARCHAR(50),
    Date DATE NOT NULL,
    Duration INT,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);


CREATE INDEX IX_Transactions_CustomerID ON Transactions(CustomerID);
CREATE INDEX IX_Engagements_CustomerID ON Engagements(CustomerID);




-- Stored procedure for customer purchase history
CREATE PROCEDURE GetCustomerPurchaseHistory
    @CustomerID INT
AS
BEGIN
    SELECT 
        c.CustomerID,
        c.Name,
        t.TransactionID,
        t.Date,
        t.Amount,
        t.ProductCategory,
        s.Location
    FROM Customers c
    JOIN Transactions t ON c.CustomerID = t.CustomerID
    JOIN Stores s ON t.StoreID = s.StoreID
    WHERE c.CustomerID = @CustomerID;
END;
GO

-- Stored procedure for engagement summary
CREATE PROCEDURE GetEngagementSummary
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SELECT 
        InteractionType,
        COUNT(*) AS InteractionCount,
        AVG(Duration) AS AvgDuration
    FROM Engagements
    WHERE Date BETWEEN @StartDate AND @EndDate
    GROUP BY InteractionType;
END;
GO


EXEC GetCustomerPurchaseHistory @CustomerID = 1;
EXEC GetEngagementSummary @StartDate = '2025-01-01', @EndDate = '2025-12-31';


SELECT*
FROM Transactions;

SELECT*
FROM Engagements;



-- Regional sales analysis
SELECT 
    c.Region,
    COUNT(DISTINCT t.CustomerID) AS UniqueCustomers,
    AVG(t.Amount) AS AvgTransactionAmount,
    SUM(t.Amount) AS TotalSales
FROM Transactions t
JOIN Customers c ON t.CustomerID = c.CustomerID
GROUP BY c.Region
ORDER BY TotalSales DESC;

-- Top 10 customers by lifetime value
SELECT TOP 10
    c.CustomerID,
    c.Name,
    SUM(t.Amount) AS CustomerLifetimeValue
FROM Transactions t
JOIN Customers c ON t.CustomerID = c.CustomerID
GROUP BY c.CustomerID, c.Name
ORDER BY CustomerLifetimeValue DESC;