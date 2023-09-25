DROP TABLE IF EXISTS Segments CASCADE;
CREATE TABLE Segments (
    Segment INTEGER PRIMARY KEY NOT NULL,
    Average_check VARCHAR NOT NULL,
    Frequency_of_purchases VARCHAR NOT NULL,
    Churn_probability VARCHAR NOT NULL
);

CALL import('Segments', '/Users/craftbec/SQL3_RetailAnalitycs_v1.0/datasets/Segments.tsv'); 


SELECT * FROM PersonalInformation
SELECT * FROM Cards
SELECT * FROM Transactions
SELECT * FROM SKUGroup
SELECT * FROM ProductGrid
SELECT * FROM Checks
SELECT * FROM Stores
SELECT * FROM DateOfAnalysisFormation
SELECT * FROM Segments

  
DROP VIEW IF EXISTS Customers_View CASCADE;

SELECT * FROM Customers_View



-- Расчет среднего чека
WITH  
Transactions_Plus AS (
SELECT cards.customer_id, cards.customer_card_id, transactions.transaction_id ,transactions.transaction_summ,
transactions.transaction_datetime, transactions.transaction_store_id
FROM transactions 
JOIN cards  ON cards.customer_card_id = transactions.customer_card_id
),
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
Average_Check_Segment.Customer_Average_Check AS Customer_Average_Check,
Average_Check_Segment.Customer_Average_Check_Segment, 
Frequency_Segment.Customer_Frequency,
Frequency_Segment.Customer_Frequency_Segment,
Inactive_Period.Customer_Inactive_Period , 
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
ON Average_Check_Segment.customer_id=Primary_Store.customer_id