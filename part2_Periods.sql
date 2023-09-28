DROP VIEW IF EXISTS Periods CASCADE;

CREATE VIEW  Periods AS
WITH 
Period AS (
SELECT PersonalInformation.customer_id, Transactions.Transaction_ID, ProductGrid.group_id, (Checks.sku_discount/Checks.sku_summ) AS Group_Min_Discount
FROM PersonalInformation
JOIN Cards ON PersonalInformation.customer_id=Cards.customer_id
JOIN Transactions ON Cards.customer_card_id = Transactions.customer_card_id
JOIN Checks  ON Checks.transaction_id = Transactions.transaction_id
JOIN ProductGrid  ON Checks.sku_id = ProductGrid.sku_id
JOIN Stores  ON ProductGrid.sku_id = Stores.sku_id AND Stores.transaction_store_id = Transactions.transaction_store_id
GROUP BY PersonalInformation.customer_id, ProductGrid.group_id, Transactions.transaction_id, Group_Min_Discount),
First_Last_Group AS (
SELECT customer_id, MIN(Transaction_DateTime) AS First_Group_Purchase_Date, MAX(Transaction_DateTime) AS Last_Group_Purchase_Date,
group_id, COUNT(Transaction_ID) AS Group_Purchase	
FROM Purchase_History
GROUP BY customer_id, group_id
),
GFrequency AS (
SELECT customer_id,group_id, ((EXTRACT (EPOCH FROM Last_Group_Purchase_Date -First_Group_Purchase_Date)/86400 + 1)/Group_Purchase) AS Group_Frequency
FROM First_Last_Group
)
SELECT  First_Last_Group.Customer_ID, First_Last_Group.Group_ID, First_Group_Purchase_Date, Last_Group_Purchase_Date, Group_Purchase, ROUND(Group_Frequency,2) AS Group_Frequency,
CASE
WHEN MAX(group_min_discount) = 0 THEN 0
ELSE (MIN(Group_Min_Discount) FILTER ( WHERE group_min_discount > 0 ))
END AS Group_Min_Discount
FROM Period
JOIN First_Last_Group ON First_Last_Group.customer_id = Period.customer_id AND Period.group_id = First_Last_Group.group_id
JOIN GFrequency ON GFrequency.customer_id = First_Last_Group.customer_id AND GFrequency.group_id = Period.group_id
GROUP BY First_Last_Group.group_id, First_Last_Group.customer_id, First_Group_Purchase_Date, Last_Group_Purchase_Date, Group_Purchase, Group_Frequency
ORDER BY First_Last_Group.customer_id, First_Last_Group.group_id;



SELECT * FROM Periods
WHERE Group_Min_Discount=0;
SELECT * FROM Periods
WHERE Group_Purchase>5;
SELECT * FROM Periods
WHERE Group_ID>2 AND Group_ID<5;
SELECT * FROM Periods
ORDER BY 4 DESC;
