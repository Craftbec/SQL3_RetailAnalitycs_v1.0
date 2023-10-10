DROP MATERIALIZED VIEW IF EXISTS Purchase_History CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS  Purchase_History AS
SELECT PersonalInformation.customer_id, Transactions.Transaction_ID, Transactions.Transaction_DateTime, ProductGrid.group_id, 
SUM(Stores.sku_purchase_price*Checks.sku_amount)::NUMERIC AS Group_Cost, SUM(Checks.sku_summ)::NUMERIC AS Group_Summ,
SUM(Checks.sku_sum_paid)::NUMERIC AS Group_Summ_Paid
FROM PersonalInformation
JOIN Cards ON PersonalInformation.customer_id=Cards.customer_id
JOIN Transactions ON Cards.customer_card_id = Transactions.customer_card_id
JOIN Checks  ON Checks.transaction_id = Transactions.transaction_id
JOIN ProductGrid  ON Checks.sku_id = ProductGrid.sku_id
JOIN Stores  ON ProductGrid.sku_id = Stores.sku_id AND Stores.transaction_store_id = Transactions.transaction_store_id
GROUP BY PersonalInformation.customer_id, Transactions.transaction_id, Transactions.transaction_datetime, ProductGrid.group_id;


--  ////////////// Tests ///////////////////

SELECT * FROM Purchase_History
WHERE Group_Cost<1000;
SELECT * FROM Purchase_History
WHERE Group_Cost<1000 AND Group_Summ_Paid>700;
SELECT * FROM Purchase_History
ORDER BY 1, 4;
SELECT * FROM Purchase_History
WHERE Group_Summ<30;
SELECT * FROM Purchase_History
WHERE Customer_id = 11;