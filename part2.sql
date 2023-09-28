DROP TABLE IF EXISTS Segments CASCADE;
CREATE TABLE Segments (
    Segment INTEGER PRIMARY KEY NOT NULL,
    Average_check VARCHAR NOT NULL,
    Frequency_of_purchases VARCHAR NOT NULL,
    Churn_probability VARCHAR NOT NULL
);

CALL import('Segments', '/Users/craftbec/SQL3_RetailAnalitycs_v1.0/datasets/Segments.tsv'); 
  
-- CLIENTS
DROP VIEW IF EXISTS Customers CASCADE;

CREATE VIEW Customers AS
-- Расчет среднего чека
WITH  
Id_Average_Check AS(
WITH tmp AS (
SELECT PersonalInformation.customer_id AS customer_id, (SUM(transaction_summ) / COUNT(transaction_summ))::numeric AS customer_average_check
FROM PersonalInformation
JOIN Cards ON PersonalInformation.customer_id = Cards.customer_id
JOIN Transactions  ON Cards.customer_card_id = Transactions.customer_card_id
GROUP BY PersonalInformation.customer_id
ORDER BY customer_average_check DESC)
SELECT row_number() over (ORDER BY customer_average_check DESC) AS roww, tmp.customer_id, tmp.customer_average_check
FROM tmp),	
--Определение сегмента.
Average_Check_Segment AS (
SELECT roww, customer_id, Customer_Average_Check,
(CASE WHEN roww <= (SELECT (max(roww) * 0.1)::bigint FROM Id_Average_Check) THEN 'High'
WHEN roww <= (SELECT (max(roww) * 0.35)::bigint FROM Id_Average_Check)
AND roww > (SELECT (max(roww) * 0.10)::bigint FROM Id_Average_Check) THEN 'Medium'
ELSE 'Low' END)::varchar AS customer_average_check_segment
FROM Id_Average_Check
),
-- Определение интенсивности транзакций
Frequency AS(
WITH tmp AS (
SELECT PersonalInformation.customer_id as customer_id,(max(date(transaction_datetime ))-min(date(transaction_datetime )))/count(transaction_id)::numeric as customer_frequency
FROM PersonalInformation
JOIN Cards  ON  PersonalInformation.customer_id = Cards.customer_id
JOIN Transactions  ON Cards.customer_card_id = Transactions.customer_card_id
GROUP BY PersonalInformation.customer_id
ORDER BY customer_frequency ASC)
SELECT row_number() over (ORDER BY customer_frequency ASC) AS roww, tmp.customer_id, tmp.customer_frequency
FROM tmp),
-- Определение сегмента
Frequency_Segment AS (
SELECT roww, customer_id, customer_frequency,
(CASE WHEN roww <= (SELECT (max(roww) * 0.1)::bigint FROM Frequency) THEN 'Often'
WHEN roww <= (SELECT (max(roww) * 0.35)::bigint FROM Frequency)
AND roww > (SELECT (max(roww) * 0.10)::bigint FROM Frequency) THEN 'Occasionally'
ELSE 'Rarely' END)::varchar AS customer_frequency_segment	
FROM Frequency
),
-- Определение периода после предыдущей транзакции
Inactive_Period AS (
SELECT PersonalInformation.customer_id as customer_id,
(EXTRACT (EPOCH FROM(SELECT * FROM DateOfAnalysisFormation))- EXTRACT(EPOCH FROM max(transaction_datetime)))/86400 AS customer_inactive_period
FROM PersonalInformation
JOIN Cards  ON  PersonalInformation.customer_id = Cards.customer_id
JOIN Transactions  ON Cards.customer_card_id = Transactions.customer_card_id
GROUP BY PersonalInformation.customer_id
ORDER BY customer_inactive_period ASC
), 
-- коэффициента оттока
Churn_Rate AS (
SELECT Inactive_Period.customer_id, Inactive_Period.customer_inactive_period/Frequency.customer_frequency::numeric AS customer_churn_rate
FROM Inactive_Period
JOIN Frequency ON Inactive_Period.customer_id = Frequency.customer_id
),
--Определение вероятности оттока
Churn_Segment AS (
SELECT *,(CASE
WHEN customer_churn_rate < 2 THEN 'Low'
WHEN customer_churn_rate >= 2 AND
customer_churn_rate < 5 THEN 'Medium'
ELSE 'High' END) AS customer_churn_segment
FROM Churn_Rate), 
--Присвоение номера сегмента
N_Segment AS (
SELECT Average_Check_Segment.customer_id, Segments.Segment AS customer_segment
FROM Average_Check_Segment
JOIN Frequency_Segment ON Average_Check_Segment.customer_id=Frequency_Segment.customer_id
JOIN Churn_Segment ON Frequency_Segment.customer_id=Churn_Segment.customer_id
JOIN Segments ON Average_Check_Segment.customer_average_check_segment=Segments.Average_check AND Frequency_Segment.customer_frequency_segment=Segments.Frequency_of_purchases AND 
Churn_Segment.customer_churn_segment=Segments.Churn_probability
),
--Определение основного магазина клиента
Transactions_Plus AS (
SELECT cards.customer_id, cards.customer_card_id, transactions.transaction_id ,transactions.transaction_summ,
transactions.transaction_datetime, transactions.transaction_store_id
FROM transactions 
JOIN cards  ON cards.customer_card_id = transactions.customer_card_id
),
Primary_Store AS (
--Общее количество транзакций
WITH Stores_Trans_Total AS (
SELECT  customer_id, count(transaction_id) AS total_trans
FROM Transactions_Plus
GROUP BY customer_id),
--количество транзакций в конкретном магазине
Stores_Trans_Con AS (
SELECT Transactions_Plus.customer_id, Transactions_Plus.transaction_store_id, count(transaction_store_id) AS trans_con, max(transaction_datetime) AS last_date
FROM Transactions_Plus
GROUP BY Transactions_Plus.customer_id, Transactions_Plus.transaction_store_id),
--соотношение транзакций в магазине к общему
Stores_Trans_Share AS (
SELECT Stores_Trans_Con.customer_id, Stores_Trans_Con.transaction_store_id, Stores_Trans_Con.trans_con, (Stores_Trans_Con.trans_con::real / Stores_Trans_Total.total_trans)::real AS trans_share, Stores_Trans_Con.last_date
FROM Stores_Trans_Con 
JOIN Stores_Trans_Total  ON Stores_Trans_Total.customer_id = Stores_Trans_Con.customer_id
ORDER BY Stores_Trans_Con.customer_id, Stores_Trans_Con.trans_con DESC),
--сортировка по доле (потом по дате)
Stores_Trans_Share_rank AS (
SELECT *, row_number() over (partition by customer_id order by trans_share DESC, last_date DESC) AS row_share_date
FROM Stores_Trans_Share),
--сортировка по дате (три последних)
Stores_Trans_three AS (
SELECT tmp.customer_id, tmp.transaction_store_id, tmp.transaction_datetime, tmp.row
FROM (SELECT *, row_number() over (partition by customer_id ORDER BY transaction_datetime DESC) row FROM transactions_plus t1) tmp
WHERE tmp.row <= 3
ORDER BY customer_id, transaction_datetime DESC),
--Последняя покупка
Last_Store_Trans AS (
SELECT Stores_Trans_three.customer_id, Stores_Trans_three.transaction_store_id, Stores_Trans_three.transaction_datetime
FROM Stores_Trans_three
WHERE Stores_Trans_three.row <= 1
ORDER BY 1),
-- Три последние транзакции совершены в одном и том же магазине
Tmp As (
SELECT customer_id , transaction_store_id, COUNT (*) AS counnt
FROM Stores_Trans_three
GROUP BY customer_id, transaction_store_id
), 
Tmp1 AS (
SELECT * FROM Tmp
WHERE counnt=3
),
--Наибольшая доля всех транзакций клиента
Tmp2 As (	
SELECT customer_id, transaction_store_id 
FROM Last_Store_Trans
EXCEPT SELECT customer_id, transaction_store_id 
FROM Tmp1
)
--Итоговый магазин
SELECT customer_id, transaction_store_id AS Customer_Primary_Store
FROM Tmp1
UNION
SELECT	customer_id, transaction_store_id AS Customer_Primary_Store
FROM Tmp2
)
SELECT Average_Check_Segment.customer_id,
Average_Check_Segment.Customer_Average_Check,
Average_Check_Segment.Customer_Average_Check_Segment, 
Frequency_Segment.Customer_Frequency,
Frequency_Segment.Customer_Frequency_Segment,
Inactive_Period.Customer_Inactive_Period, 
Churn_Segment.Customer_Churn_Rate, 
Churn_Segment.Customer_Churn_Segment,
N_Segment.Customer_Segment,
Primary_Store.Customer_Primary_Store
FROM Average_Check_Segment
JOIN Frequency_Segment
ON Average_Check_Segment.customer_id=Frequency_Segment.customer_id
JOIN Inactive_Period 
ON Average_Check_Segment.customer_id=Inactive_Period.customer_id
JOIN Churn_Segment
ON Average_Check_Segment.customer_id=Churn_Segment.customer_id
JOIN N_Segment
ON Average_Check_Segment.customer_id=N_Segment.customer_id
JOIN Primary_Store
ON Average_Check_Segment.customer_id=Primary_Store.customer_id;



SELECT * FROM customers
WHERE customer_primary_store = 1;
SELECT * FROM customers
WHERE customer_churn_segment = 'Low';
SELECT * FROM customers
WHERE customer_frequency > 100;
SELECT * FROM customers
WHERE customer_inactive_period > 250 AND customer_churn_segment='Low';
SELECT * FROM customers
WHERE customer_frequency_segment = 'Occasionally'
ORDER BY customer_id DESC;





DROP VIEW IF EXISTS Purchase_History CASCADE;

CREATE VIEW  Purchase_History AS
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



SELECT * FROM Purchase_History
WHERE Group_Cost<1000;
SELECT * FROM Purchase_History
WHERE Group_Cost<1000 AND Group_Summ_Paid>700;
SELECT * FROM Purchase_History
ORDER BY 1, 4;
SELECT * FROM Purchase_History
WHERE Group_Summ<15;


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
WHERE Group_ID>10 AND Group_ID<30;
SELECT * FROM Periods
ORDER BY 4 DESC;




-- Расчет востребованности
WITH
Affinity_Index AS (
SELECT Purchase_History.customer_id, Periods.group_id, 
Periods.group_purchase/COUNT(Purchase_History.transaction_id)::NUMERIC AS Group_Affinity_Index
FROM Purchase_History
JOIN Periods  ON Periods.customer_id = Purchase_History.customer_id
WHERE Purchase_History.transaction_datetime BETWEEN first_group_purchase_date AND last_group_purchase_date
GROUP BY Purchase_History.customer_id, Periods.group_id, Periods.group_purchase),
-- Расчет индекса оттока из группы
Churn_Rate AS (
SELECT Purchase_History.customer_id, Periods.group_id, 
((EXTRACT(epoch FROM(SELECT * FROM DateOfAnalysisFormation)) - EXTRACT(epoch FROM MAX(transaction_datetime))))/(Periods.group_frequency)/86400::NUMERIC AS Group_Churn_Rate
FROM Purchase_History
JOIN Periods ON Purchase_History.customer_id=Periods.customer_id AND Purchase_History.group_id=Periods.group_id
GROUP BY  Purchase_History.customer_id, Periods.group_id, Periods.group_frequency
),
-- Расчет интервалов потребления группы.
Intervals AS (
	SELECT customer_id, transaction_id,  group_id, transaction_datetime,
 	  EXTRACT(DAY FROM (transaction_datetime - LAG(transaction_datetime)
 	OVER (PARTITION BY customer_id, group_id ORDER BY transaction_datetime))) AS interval
	FROM Purchase_History
	  GROUP BY customer_id, transaction_id, group_id, transaction_datetime
	ORDER BY customer_id,transaction_datetime
),
-- Расчет стабильности потребления группы
Stable_Consumption AS (
	SELECT Intervals.customer_id,  Intervals.group_id,
	 COALESCE(
	AVG(CASE
 WHEN  Intervals.interval-Periods.group_frequency < 0::NUMERIC 
 	THEN ( Intervals.interval-Periods.group_frequency) * -1::NUMERIC
 ELSE  Intervals.interval-Periods.group_frequency
	END / Periods.group_frequency), 0)AS Group_Stability_Index
	FROM Intervals
	JOIN Periods ON Intervals.customer_id=Periods.customer_id AND Periods.group_id=Intervals.group_id
	GROUP BY Intervals.customer_id, Intervals.group_id
),
-- Расчет фактической маржи по группе для клиента
 Margin AS (
	 SELECT customer_id, group_id , SUM(Group_Summ_Paid-group_cost) AS Group_Margin
	 FROM Purchase_History
	 GROUP BY customer_id, group_id 
), 
-- Определение количества транзакций клиента со скидкой
Count_Discount AS (
	SELECT DISTINCT PersonalInformation.customer_id, ProductGrid.group_id, COUNT (Checks.transaction_id)
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
	SELECT customer_id,  group_id, MIN(group_min_discount) AS Group_Minimum_Discount
	FROM Periods
	WHERE group_min_discount>0
	GROUP BY customer_id,  group_id
),
-- Определение среднего размера скидки по группе
Average_discount_amount AS (
SELECT  customer_id, group_id, AVG(group_summ_paid/group_summ)::NUMERIC AS Group_Average_Discount
FROM purchase_history
JOIN Checks ON purchase_history.transaction_id = Checks.transaction_id
WHERE sku_discount > 0
GROUP BY customer_id, group_id
)

SELECT Affinity_Index.customer_id, Affinity_Index.group_id, group_affinity_index, Group_Churn_Rate, Group_Stability_Index,Group_Margin, Group_Discount_Share,Group_Minimum_Discount
Group_Average_Discount
FROM Affinity_Index
JOIN Churn_Rate ON Affinity_Index.group_id = Churn_Rate.group_id AND Affinity_Index.customer_id = Churn_Rate.customer_id
JOIN Stable_Consumption ON Stable_Consumption.group_id = Affinity_Index.group_id AND Stable_Consumption.customer_id = Affinity_Index.customer_id
JOIN Margin  ON Margin.customer_id = Affinity_Index.customer_id AND Margin.group_id = Affinity_Index.group_id
JOIN Count_Discount_Share  ON Count_Discount_Share.group_id = Affinity_Index.group_id AND Count_Discount_Share.customer_id = Affinity_Index.customer_id
JOIN Min_Discount  ON Min_Discount.group_id = Affinity_Index.group_id AND Min_Discount.customer_id = Affinity_Index.customer_id
JOIN Average_discount_amount  ON Affinity_Index.group_id = Average_discount_amount.group_id AND Affinity_Index.customer_id = Average_discount_amount.customer_id;