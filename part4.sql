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



CREATE OR REPLACE FUNCTION test(ma date, mi date, average_factor NUMERIC, n1 NUMERIC, n2 NUMERIC, n3 NUMERIC)
RETURNS TABLE (Customer_ID INTEGER, Required_Check_Measure NUMERIC, Group_Name VARCHAR, Offer_Discount_Depth NUMERIC)
AS $$
BEGIN
        IF (ma > GetDates(1)) THEN
            ma = GetDates(1);
        ELSEIF (mi < GetDates(2)) THEN
            mi = GetDates(2);
        ELSEIF (mi >= ma) THEN
            RAISE EXCEPTION
                'Некорректный период';
        END IF;
		RETURN QUERY
		WITH res1 AS (
SELECT Personalinformation.customer_id AS c_id, (SUM(SKU_Summ)/COUNT(Personalinformation.customer_id)*average_factor)::NUMERIC AS Required
FROM Personalinformation
JOIN Cards ON Personalinformation.customer_id = Cards.customer_id
JOIN Transactions ON Cards.customer_card_id = Transactions.customer_card_id
JOIN Checks ON Checks.Transaction_id = Transactions.Transaction_id
WHERE date(transaction_datetime) >= mi AND date(transaction_datetime)<= ma
GROUP BY Personalinformation.customer_id)
SELECT Customer, Required , G_Name , G_Discount NUMERIC 
FROM res1
JOIN nnn(n1, n2,n3) tmp ON res1.c_id = tmp.Customer;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION nnn(n1 NUMERIC, n2 NUMERIC, n3 NUMERIC) RETURNS TABLE (Customer INTEGER, G_Name VARCHAR, G_Discount NUMERIC)
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


SELECT * FROM test('2023-08-20', '2015-01-20', 3, 10, 3,3)