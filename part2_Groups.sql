DROP MATERIALIZED VIEW IF EXISTS Groups CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS Groups AS
WITH
PP AS (
SELECT Purchase_History.customer_id,  Transaction_id, Transaction_DateTime, Purchase_History.group_id, Group_Cost, Group_Summ, Group_Summ_Paid, First_Group_Purchase_Date, Last_Group_Purchase_Date,
Group_Purchase, Group_Frequency, Group_Min_Discount
FROM Purchase_History
JOIN Periods  ON Periods.customer_id = Purchase_History.customer_id AND Periods.group_id = Purchase_History.group_id
),
-- Расчет востребованности
Affinity_Index AS (
WITH 
COUN AS (
SELECT customer_id,  COUNT(DISTINCT transaction_id) AS all_transaction
FROM PP
WHERE transaction_datetime BETWEEN first_group_purchase_date AND last_group_purchase_date
GROUP BY customer_id)
SELECT PP.customer_id,  PP.group_id , group_purchase/all_transaction::NUMERIC  AS Group_Affinity_Index
FROM PP
JOIN COUN ON COUN.customer_id= PP.customer_id
GROUP BY PP.customer_id,  PP.group_id , Group_Affinity_Index
),
-- Расчет индекса оттока из группы / Расчет фактической маржи по группе для клиента
Churn_Rate AS (
SELECT customer_id, group_id, 
((EXTRACT(epoch FROM(SELECT * FROM DateOfAnalysisFormation)) - EXTRACT(epoch FROM MAX(transaction_datetime))))/(group_frequency)/86400::NUMERIC AS Group_Churn_Rate , 
SUM(Group_Summ_Paid-group_cost) AS Group_Margin	
FROM PP
GROUP BY  customer_id, group_id, group_frequency
),
-- Расчет интервалов потребления группы.
Intervals AS (
SELECT customer_id, transaction_id,  group_id, transaction_datetime,
EXTRACT(DAY FROM (transaction_datetime - LAG(transaction_datetime)
OVER (PARTITION BY customer_id, group_id ORDER BY transaction_datetime))) AS interval
FROM PP
GROUP BY customer_id, transaction_id, group_id, transaction_datetime
ORDER BY customer_id,transaction_datetime
),
-- Расчет стабильности потребления группы
Stable_Consumption AS (
SELECT Intervals.customer_id,  Intervals.group_id,
COALESCE(AVG(CASE
WHEN  Intervals.interval-PP.group_frequency < 0::NUMERIC 
THEN ( Intervals.interval-PP.group_frequency) * -1::NUMERIC
ELSE  Intervals.interval-PP.group_frequency
END / PP.group_frequency), 0)AS Group_Stability_Index
FROM Intervals
JOIN PP ON Intervals.customer_id=PP.customer_id AND PP.group_id=Intervals.group_id
GROUP BY Intervals.customer_id, Intervals.group_id
),
-- Определение количества транзакций клиента со скидкой
Count_Discount AS (
	SELECT  PersonalInformation.customer_id, ProductGrid.group_id, COUNT (DISTINCT Checks.transaction_id)
	FROM PersonalInformation
    JOIN Cards  ON PersonalInformation.customer_id = Cards.customer_id
    JOIN Transactions  ON Cards.customer_card_id = Transactions.customer_card_id
    JOIN Checks  ON Transactions.transaction_id = Checks.transaction_id
    JOIN ProductGrid  ON ProductGrid.sku_id = Checks.sku_id
	WHERE Checks.sku_discount>0
	GROUP BY PersonalInformation.customer_id, ProductGrid.group_id
),
Count_Discount_Share AS (
 SELECT Count_Discount.customer_id, Count_Discount.group_id, Count_Discount.count::NUMERIC/Periods.group_purchase::NUMERIC AS Group_Discount_Share
	 FROM Count_Discount
	 JOIN Periods  ON Count_Discount.group_id = Periods.group_id and Count_Discount.customer_id = Periods.customer_id
	GROUP BY Count_Discount.customer_id, Count_Discount.group_id, Group_Discount_Share
 ),
 -- Определение минимального размера скидки по группе
Min_Discount AS (
	SELECT customer_id,  group_id, MIN(group_min_discount)::NUMERIC AS Group_Minimum_Discount
	FROM Periods
	WHERE group_min_discount>0
	GROUP BY customer_id,  group_id
),
-- Определение среднего размера скидки по группе
Average_discount_amount AS (
SELECT  customer_id, group_id, AVG(group_summ_paid/group_summ)::NUMERIC AS Group_Average_Discount
FROM PP
JOIN Checks ON PP.transaction_id = Checks.transaction_id
WHERE sku_discount > 0
GROUP BY customer_id, group_id
)
SELECT Affinity_Index.customer_id, Affinity_Index.group_id, ROUND(group_affinity_index,2) AS group_affinity_index, ROUND(Group_Churn_Rate,2) AS Group_Churn_Rate,
ROUND(Group_Stability_Index,2) AS Group_Stability_Index,
ROUND(Group_Margin,2) AS Group_Margin, ROUND(Group_Discount_Share,2) AS Group_Discount_Share, ROUND(Group_Minimum_Discount,2) AS Group_Minimum_Discount,
ROUND(Group_Average_Discount,2) AS Group_Average_Discount
FROM Affinity_Index
JOIN Churn_Rate ON Affinity_Index.group_id = Churn_Rate.group_id AND Affinity_Index.customer_id = Churn_Rate.customer_id
JOIN Stable_Consumption ON Stable_Consumption.group_id = Affinity_Index.group_id AND Stable_Consumption.customer_id = Affinity_Index.customer_id
JOIN Count_Discount_Share  ON Count_Discount_Share.group_id = Affinity_Index.group_id AND Count_Discount_Share.customer_id = Affinity_Index.customer_id
JOIN Min_Discount  ON Min_Discount.group_id = Affinity_Index.group_id AND Min_Discount.customer_id = Affinity_Index.customer_id
JOIN Average_discount_amount  ON Affinity_Index.group_id = Average_discount_amount.group_id AND Affinity_Index.customer_id = Average_discount_amount.customer_id;


SELECT * FROM Groups
WHERE Group_Average_Discount>0.9;
SELECT * FROM Groups
WHERE customer_id=1;
SELECT * FROM Groups
WHERE group_margin<0;
SELECT * FROM Groups
WHERE group_margin>1000;