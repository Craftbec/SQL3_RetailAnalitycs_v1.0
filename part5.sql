CREATE OR REPLACE FUNCTION OffersAimedFrequencyVisits(first_date DATE, last_date DATE, number_of_transactions NUMERIC, maximum_churn_index NUMERIC, maximum_share  NUMERIC, margin_share NUMERIC)
RETURNS TABLE (Customer_ID INTEGER, Start_Date DATE, End_Date DATE, Required_Transactions_Count NUMERIC, Group_Name VARCHAR, Offer_Discount_Depth NUMERIC)
AS $$
BEGIN
RETURN QUERY
SELECT  Personalinformation.customer_id  AS c_id, first_date, last_date,  ROUND((last_date-first_date)/Customer_Frequency)+number_of_transactions, 
GG.G_Name, GG.G_Discount
FROM Personalinformation
JOIN GetGroupNameAndDiscount(maximum_churn_index, maximum_share, margin_share)  GG ON Personalinformation.customer_id = GG.Customer
JOIN Customers ON Customers.customer_id = Personalinformation.customer_id;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM OffersAimedFrequencyVisits('2021-08-20', '2023-01-20', 100, 40, 3, 3);