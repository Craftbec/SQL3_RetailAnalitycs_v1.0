CREATE OR REPLACE FUNCTION Choice(number_of_groups NUMERIC, maximum_churn_index NUMERIC, maximum_consumption_stability NUMERIC)
RETURNS TABLE (cos_id INTEGER, SKU_Name VARCHAR,  G_id BIGINT, Offer_Discount_Depth NUMERIC)
AS $$
BEGIN
RETURN QUERY
WITH
res AS (
WITH 
tmp AS (
SELECT Personalinformation.customer_id, group_id, Group_Affinity_Index,
row_number() over (partition by Personalinformation.customer_id order by Group_Affinity_Index DESC) as rank
FROM Personalinformation
JOIN Groups ON Personalinformation.customer_id = Groups.customer_id
WHERE Group_Churn_Rate < maximum_churn_index AND Group_Stability_Index < maximum_consumption_stability), 
tmp2 AS (
SELECT customer_id,  group_id, MIN(Group_Min_Discount) AS minn
FROM periods
GROUP BY customer_id,  group_id
)SELECT tmp.customer_id AS c_id, group_name, tmp.group_id AS g_id, (ROUND(minn*100./5.+0.5)*5)::NUMERIC AS G_Discount FROM tmp
JOIN tmp2 ON tmp.customer_id=tmp2.customer_id AND tmp.group_id=tmp2.group_id
JOIN SkuGroup ON SkuGroup.group_id=tmp.group_id
WHERE rank<=number_of_groups)
SELECT * FROM res;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION CrossSelling(number_of_groups NUMERIC, maximum_churn_index NUMERIC, maximum_consumption_stability  NUMERIC, maximum_sku NUMERIC,  margin_share NUMERIC)
RETURNS TABLE ("Customer_ID" INTEGER, "SKU_Name" VARCHAR,  "Offer_Discount_Depth" NUMERIC)
AS $$
BEGIN
RETURN QUERY
WITH
res AS (
SELECT * FROM Choice(number_of_groups,maximum_churn_index,maximum_consumption_stability)), 
tmp AS (
SELECT Customers.customer_id, Customer_Primary_Store, ProductGrid.group_id, Checks.sku_id, SKU_Retail_Price,  SKU_Retail_Price-SKU_Purchase_Price AS max_mar                                                 
FROM Customers
JOIN Cards ON Customers.customer_id = Cards.customer_id
JOIN Transactions ON Cards.customer_card_id = Transactions.customer_card_id
JOIN Checks ON Checks.Transaction_id = Transactions.Transaction_id
JOIN ProductGrid ON ProductGrid.sku_id = Checks.sku_id
JOIN Stores ON Stores.sku_id = ProductGrid.sku_id),
ttt AS (
SELECT res.cos_id, res.g_id, sku_id, sku_name, Offer_Discount_Depth, SKU_Retail_Price,  max_mar,
row_number() over (partition by res.cos_id, res.g_id order by max_mar DESC) as rank
FROM res
JOIN tmp ON res.cos_id=tmp.customer_id AND res.g_id=tmp.group_id
),
COUN AS (
SELECT customer_id, ProductGrid.group_id, ProductGrid.sku_id
FROM Cards
JOIN Transactions ON Cards.customer_card_id = Transactions.customer_card_id
JOIN Checks ON transactions.transaction_id = Checks.transaction_id
JOIN ProductGrid ON ProductGrid.sku_id = Checks.sku_id
JOIN Skugroup ON Skugroup.group_id = ProductGrid.group_id), 
Share_sku AS (
WITH 
tmp AS (SELECT COUN.customer_id, COUN.group_id, COUNT(*) AS t1
FROM COUN
JOIN ttt ON COUN.customer_id=ttt.cos_id AND COUN.group_id=ttt.g_id AND COUN.sku_id = ttt.sku_id
WHERE rank=1 
GROUP BY COUN.customer_id, COUN.group_id
), tmp2 AS (
SELECT COUN.customer_id, COUN.group_id, COUNT(*) AS t2
FROM COUN
GROUP BY  COUN.customer_id, COUN.group_id
)
SELECT  tmp.customer_id, tmp.group_id , t1/t2::NUMERIC AS sc
FROM tmp
JOIN tmp2 ON tmp.customer_id=tmp2.customer_id AND tmp.group_id=tmp2.group_id
),
Mar AS (
SELECT * FROM Share_sku
WHERE sc <=maximum_sku
),
ok AS (
SELECT ttt.cos_id, sku_name as nam, ttt.g_id, sku_id, Offer_Discount_Depth AS dis,  max_mar*margin_share/sku_retail_price AS ert
FROM ttt
JOIN Mar ON Mar.customer_id= ttt.cos_id AND Mar.group_id=ttt.g_id
WHERE rank=1)
SELECT cos_id,  nam, dis
FROM ok
WHERE dis<ert;
END;
$$ LANGUAGE plpgsql;




SELECT * FROM CrossSelling(2, 100, 100, 1, 40);