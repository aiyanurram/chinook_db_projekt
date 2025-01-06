CREATE DATABASE IF NOT EXISTS TIGER_CHINOOK;
USE DATABASE TIGER_CHINOOK;

CREATE OR REPLACE STAGE MY_STAGE FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

create or replace TABLE artist (
    artistid NUMBER(38,0),
    name VARCHAR(120)
);

create or replace TABLE album (
    albumid NUMBER(38,0),
    title VARCHAR(160),
    artist NUMBER(38,0)
);

create or replace TABLE track (
    trackid NUMBER(38,0),
    name VARCHAR(200),
    albumid NUMBER(38,0),
    mediatypeid NUMBER(38,0),
    genreid NUMBER(38,0),
    composer VARCHAR(220),
    milliseconds NUMBER(38,0),
    bytes NUMBER(38,0),
    unitprice NUMBER(10,2)
);

create or replace TABLE customer (
    customerid NUMBER(38,0),
    firstname VARCHAR(40),
    lastname VARCHAR(20),
    company VARCHAR(80),
    address VARCHAR(70),
    city VARCHAR(40),
    state VARCHAR(40),
    country VARCHAR(40),
    postalcode VARCHAR(10),
    phone VARCHAR(24),
    fax VARCHAR(24),
    email VARCHAR(60),
    supportrepid NUMBER(38,0)
);

create or replace TABLE invoice (
    invoiceid NUMBER(38,0),
    customerid NUMBER(38,0),
    invoicedate TIMESTAMP,
    billingaddress VARCHAR(70),
    billingcity VARCHAR(40),
    billingstate VARCHAR(40),
    billingcountry VARCHAR(40),
    billingpostalcode VARCHAR(10),
    total NUMBER(10,2)
);

create or replace TABLE invoiceline (
    invoicelineid NUMBER(38,0),
    invoiceid NUMBER(38,0),
    trackid NUMBER(38,0),
    unitprice NUMBER(10,2),
    quantity NUMBER(38,0)
);

create or replace TABLE employee (
    employeeid NUMBER(38,0),
    lastname VARCHAR(20),
    firstname VARCHAR(20),
    title VARCHAR(30),
    reportsto NUMBER(38,0),
    birthdate DATE,
    hiredate DATE,
    address VARCHAR(70),
    city VARCHAR(40),
    state VARCHAR(40),
    country VARCHAR(40),
    postalcode VARCHAR(10),
    phone VARCHAR(24),
    fax VARCHAR(24),
    email VARCHAR(60)
);

create or replace TABLE mediatype (
    mediatypeid NUMBER(38,0),
    name VARCHAR(120)
);

create or replace TABLE genre (
    genreid NUMBER(38,0),
    name VARCHAR(120)
);

create or replace TABLE playlist (
    playlist_id NUMBER(38,0),
    name VARCHAR(120)
);

create or replace TABLE playlisttrack (
    playlist_id NUMBER(38,0),
    track_id NUMBER(38,0)
);

COPY INTO album
FROM @MY_STAGE/album.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO artist
FROM @MY_STAGE/artist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO customer
FROM @MY_STAGE/customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employee
FROM @MY_STAGE/employee.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO genre
FROM @MY_STAGE/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoiceline
FROM @MY_STAGE/invoiceline.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO invoice
FROM @MY_STAGE/invoice.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO mediatype
FROM @MY_STAGE/mediatype.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO playlist
FROM @MY_STAGE/playlist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO playlisttrack
FROM @MY_STAGE/playlisttrack.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO track
FROM @MY_STAGE/track.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);



SELECT * FROM album;
SELECT * FROM artist;
SELECT * FROM customer;
SELECT * FROM employee;
SELECT * FROM genre;
SELECT * FROM invoice;
SELECT * FROM invoiceline;
SELECT * FROM mediatype;
SELECT * FROM playlist; 
SELECT * FROM playlisttrack;
SELECT * FROM track;


CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY InvoiceDate) AS TimeId,
    EXTRACT(YEAR FROM CAST(InvoiceDate AS TIMESTAMP)) AS year,
    EXTRACT(MONTH FROM CAST(InvoiceDate AS TIMESTAMP)) AS month,
    EXTRACT(DAY FROM CAST(InvoiceDate AS TIMESTAMP)) AS day,
FROM invoice; 

CREATE OR REPLACE TABLE dim_address AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY PostalCode) AS AddressId,
    State,
    Country,
    PostalCode
FROM customer;

SELECT * FROM dim_employee;

CREATE OR REPLACE TABLE dim_employee AS
SELECT DISTINCT
    EmployeeId AS EmployeeId,
    FirstName,
    LastName,
    DATEDIFF(YEAR, BirthDate, CURRENT_DATE) AS Age
FROM employee;

CREATE OR REPLACE TABLE dim_customer AS
SELECT DISTINCT
    CustomerId,
    FirstName,
    LastName,
    Company,
    NULL AS Age,
    NULL AS Gender
FROM customer;

CREATE OR REPLACE TABLE dim_track AS
SELECT DISTINCT
    TrackId,
    Name AS TrackName,
    Composer,
    Bytes,
    NULL AS Artist
FROM track;

CREATE OR REPLACE TABLE sales_fact AS
SELECT
    ROW_NUMBER() OVER (ORDER BY i.InvoiceId) AS Sales_factId,
    il.Quantity AS Quantity,
    il.UnitPrice AS UnitPrice,
    il.Quantity * il.UnitPrice AS Total,
    d.TimeId AS dim_time_TimeId,
    c.CustomerId AS dim_customer_CustomerId,
    e.EmployeeId AS dim_employee_EmployeeId,
    a.AddressId AS dim_address_AddressId,
    t.TrackId AS dim_track_TrackId
FROM
    invoice i
JOIN
    invoiceline il ON i.InvoiceId = il.InvoiceId
JOIN
    dim_customer c ON i.CustomerId = c.CustomerId
JOIN
    dim_employee e ON e.EmployeeId = e.EmployeeId
LEFT JOIN
    dim_address a ON i.BillingPostalCode = a.PostalCode
LEFT JOIN
    dim_time d ON DATE(i.InvoiceDate) = DATE(CONCAT(d.year, '-', d.month, '-', d.day))
JOIN
    dim_track t ON il.TrackId = t.TrackId;

    
CREATE OR REPLACE TABLE bridge_track_sales AS
SELECT DISTINCT
    t.TrackId AS dim_track_TrackId,
    s.Sales_factId AS sales_fact_Sales_factId
FROM dim_track t
JOIN sales_fact s
    ON t.TrackId = s.dim_track_TrackId;

DROP TABLE IF EXISTS album;
DROP TABLE IF EXISTS artist;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS employee;
DROP TABLE IF EXISTS genre;
DROP TABLE IF EXISTS invoiceline;
DROP TABLE IF EXISTS invoice;
DROP TABLE IF EXISTS mediatype;
DROP TABLE IF EXISTS playlisttrack;
DROP TABLE IF EXISTS track;

---1

CREATE OR REPLACE VIEW peak_minimum_orders AS
SELECT
    d.year AS Year,
    d.month AS Month,
    COUNT(s.Sales_factId) AS Total_Orders
FROM 
    sales_fact s
JOIN 
    dim_time d ON s.dim_time_TimeId = d.TimeId
GROUP BY 
    d.year, d.month
ORDER BY 
    Total_Orders DESC;

SELECT * FROM peak_minimum_orders;

---2

CREATE OR REPLACE VIEW orders_by_country AS
SELECT 
    da.Country AS Country,
    COUNT(s.Sales_factId) AS TotalOrders
FROM 
    sales_fact s
JOIN 
    dim_address da ON s.dim_address_AddressId = da.AddressId
GROUP BY  
    da.Country
ORDER BY 
    TotalOrders DESC
LIMIT 10;

SELECT * FROM orders_by_country;

---3

CREATE OR REPLACE VIEW employees_by_country AS
SELECT 
    da.Country AS Country,
    COUNT(de.EmployeeId) AS TotalEmployees
FROM 
    dim_employee de
JOIN 
    dim_address da ON de.EmployeeId = da.AddressId
GROUP BY 
    da.Country
ORDER BY 
    TotalEmployees DESC
LIMIT 10;

SELECT * FROM employees_by_country;

---4

CREATE OR REPLACE VIEW most_popular_track AS
SELECT 
    dt.TrackName AS TrackName,
    COUNT(s.Sales_factId) AS OrderCount
FROM 
    dim_track dt
JOIN 
    sales_fact s ON dt.TrackId = s.dim_track_TrackId
GROUP BY 
    dt.TrackName
ORDER BY 
    OrderCount DESC
LIMIT 10;

SELECT * FROM most_popular_track;

--5
CREATE OR REPLACE VIEW top_loyal_customers AS
SELECT 
    dc.CustomerId AS CustomerId,
    CONCAT(dc.FirstName, ' ', dc.LastName) AS CustomerName,
    COUNT(s.Sales_factId) AS TotalOrders,
    SUM(s.Total) AS TotalSpent
FROM 
    dim_customer dc
JOIN 
    sales_fact s ON dc.CustomerId = s.dim_customer_CustomerId
GROUP BY 
    dc.CustomerId, CustomerName
ORDER BY 
    TotalOrders DESC
LIMIT 10;

SELECT * FROM top_loyal_customers;
