CREATE OR REPLACE FUNCTION Choice(number_of_groups NUMERIC, maximum_churn_index NUMERIC, maximum_consumption_stability NUMERIC)
RETURNS TABLE (cos_id INTEGER, S_Name VARCHAR,  G_id BIGINT, dis NUMERIC)
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




CREATE MATERIALIZED VIEW IF NOT EXISTS MaximumMargin AS
SELECT Customers.customer_id, Customer_Primary_Store, ProductGrid.group_id, Checks.sku_id, SKU_Retail_Price,  SKU_Retail_Price-SKU_Purchase_Price AS max_mar                                                 
FROM Customers
JOIN Cards ON Customers.customer_id = Cards.customer_id
JOIN Transactions ON Cards.customer_card_id = Transactions.customer_card_id
JOIN Checks ON Checks.Transaction_id = Transactions.Transaction_id
JOIN ProductGrid ON ProductGrid.sku_id = Checks.sku_id
JOIN Stores ON Stores.sku_id = ProductGrid.sku_id;



CREATE MATERIALIZED VIEW IF NOT EXISTS ShareSku AS
WITH 
tmp AS (
SELECT customer_id, ProductGrid.group_id, ProductGrid.sku_id
FROM Cards
JOIN Transactions ON Cards.customer_card_id = Transactions.customer_card_id
JOIN Checks ON transactions.transaction_id = Checks.transaction_id
JOIN ProductGrid ON ProductGrid.sku_id = Checks.sku_id
JOIN Skugroup ON Skugroup.group_id = ProductGrid.group_id), 
Al AS (
SELECT customer_id, group_id, COUNT(*) AS al 
FROM tmp
GROUP BY customer_id, group_id),
G_id AS (
SELECT customer_id, group_id, sku_id, COUNT(*) AS g_i 
FROM tmp
GROUP BY customer_id, group_id, sku_id)
SELECT Al.customer_id AS cos_id, Al.group_id AS g_id, sku_id, ROUND(g_i/al::NUMERIC,2) AS share_sku
FROM Al
JOIN G_id ON Al.customer_id=G_id.customer_id AND Al.group_id= G_id.group_id;


CREATE OR REPLACE FUNCTION CrossSelling(number_of_groups NUMERIC, maximum_churn_index NUMERIC, maximum_consumption_stability  NUMERIC, maximum_sku NUMERIC,  margin_share NUMERIC)
RETURNS TABLE (Customer_ID INTEGER, SKU_Name VARCHAR,  Offer_Discount_Depth NUMERIC)
AS $$
BEGIN
RETURN QUERY
WITH
tmp AS (
SELECT cos_id, g_id, sku_id, s_name, dis, SKU_Retail_Price,  max_mar,
row_number() over (partition by cos_id, g_id order by max_mar DESC) as rank
FROM Choice(number_of_groups,maximum_churn_index,maximum_consumption_stability) tm
JOIN (SELECT * FROM MaximumMargin) tmp ON tm.g_id=tmp.group_id), 
res AS(
SELECT tmp.cos_id, s_name, dis, ROUND(margin_share*max_mar/SKU_Retail_Price, 2) AS tt
FROM tmp
JOIN (SELECT * FROM ShareSku) sh ON tmp.cos_id = sh.cos_id AND tmp.g_id=sh.g_id AND tmp.sku_id = sh.sku_id
WHERE rank = 1 AND share_sku<= maximum_sku)
SELECT cos_id, s_name, dis
FROM res
WHERE dis<tt;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM CrossSelling(3, 100, 100, 1, 40);