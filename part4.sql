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



CREATE OR REPLACE FUNCTION test(ma date, mi date, average_factor NUMERIC) RETURNS TABLE (Customer_ID INTEGER, Required_Check_Measure NUMERIC)
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
SELECT Personalinformation.customer_id AS c_id, (SUM(SKU_Summ)/COUNT(Personalinformation.customer_id)*average_factor)::NUMERIC AS Required
FROM Personalinformation
JOIN Cards ON Personalinformation.customer_id = Cards.customer_id
JOIN Transactions ON Cards.customer_card_id = Transactions.customer_card_id
JOIN Checks ON Checks.Transaction_id = Transactions.Transaction_id
WHERE date(transaction_datetime) >= '2015-01-20' AND date(transaction_datetime)<= '2023-08-20'
GROUP BY Personalinformation.customer_id;
END;
$$ LANGUAGE plpgsql;






SELECT customer_id, group_id, Group_Affinity_Index , Group_Churn_Rate, Group_Discount_Share, ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY Group_Affinity_Index DESC) AS rank
FROM groups
WHERE Group_Churn_Rate <= 1.2 AND Group_Discount_Share <=3