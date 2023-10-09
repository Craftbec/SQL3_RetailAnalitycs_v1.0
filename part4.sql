DROP FUNCTION IF EXISTS GetDates(integer);
CREATE FUNCTION GetDates(key integer)
RETURNS SETOF date
AS $$
BEGIN
IF (key = 1) THEN
RETURN QUERY
SELECT MAX(date(transaction_datetime))
FROM transactions;
ELSEIF (key = 2) THEN
RETURN QUERY
SELECT MIN(date(transaction_datetime))
FROM transactions;
END IF;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION GetGroupNameAndDiscount(n1 NUMERIC, n2 NUMERIC, n3 NUMERIC) 
RETURNS TABLE (Customer INTEGER, G_Name VARCHAR, G_Discount NUMERIC)
AS $$
BEGIN
RETURN QUERY
WITH
tmp AS (
WITH tt AS (
SELECT group_id,  AVG(Group_Margin) AS mar
FROM groups
GROUP BY group_id
)
SELECT  customer_id ,groups.group_id, groups.group_affinity_index, mar/100.*n1 AS mar
FROM groups
JOIN tt ON tt.group_id = groups.group_id
WHERE Group_Churn_Rate <= n2 AND Group_Discount_Share <=n3 ),
tmp2 AS (
WITH tt AS (
SELECT customer_id,  group_id, MIN(Group_Min_Discount) AS minn
FROM periods
GROUP BY customer_id,  group_id
)
SELECT customer_id,  group_id, (ROUND(minn*100./5.+0.5)*5)::NUMERIC AS ddd
FROM tt
), 
Res AS (
select tmp.customer_id,  tmp.group_id, group_affinity_index, mar, ddd,
row_number() over (partition by tmp.customer_id order by group_affinity_index DESC) as rank
from tmp
JOIN tmp2
ON tmp.customer_id=tmp2.customer_id AND tmp.group_id=tmp2.group_id  AND ddd < mar)
SELECT customer_id, Skugroup.group_name, ddd AS Offer_Discount_Depth
FROM Res
JOIN Skugroup ON Res.group_id=Skugroup.group_id
WHERE rank = 1;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION OffersGrowthCheck(first_date DATE, last_date DATE, average_factor NUMERIC, maximum_churn_index NUMERIC, maximum_share  NUMERIC, margin_share NUMERIC)
RETURNS TABLE (Customer_ID BIGINT, Required_Check_Measure NUMERIC, Group_Name VARCHAR, Offer_Discount_Depth NUMERIC)
AS $$
BEGIN
IF (last_date > GetDates(1)) THEN
last_date = GetDates(1);
ELSEIF (first_date < GetDates(2)) THEN
first_date = GetDates(2);
ELSEIF (first_date >= last_date) THEN
RAISE EXCEPTION
'Некорректный период';
END IF;
RETURN QUERY
WITH 
Counn AS (
SELECT Cards.Customer_id AS c_id, SUM(Transactions.Transaction_summ)/COUNT(*) AS cc
FROM Cards
JOIN Transactions ON Cards.customer_card_id = Transactions.customer_card_id
WHERE date(transaction_datetime) >= first_date AND date(transaction_datetime)<= last_date
GROUP BY Cards.Customer_id
)
SELECT c_id,  ROUND(cc::NUMERIC*average_factor,2) AS Req, g_name, g_discount
FROM Counn
JOIN GetGroupNameAndDiscount(maximum_churn_index, maximum_share,margin_share) tmp ON Counn.c_id = tmp.Customer;
END;
$$ LANGUAGE plpgsql;





SELECT * FROM OffersGrowthCheck('2015-01-20', '2023-08-20', 3, 40 ,3, 3);