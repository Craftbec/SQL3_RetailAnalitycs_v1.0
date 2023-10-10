DROP TABLE IF EXISTS PersonalInformation, Cards, Transactions, SKUGroup, ProductGrid, Checks, Stores, DateOfAnalysisFormation CASCADE;


CREATE TABLE PersonalInformation(
  Customer_ID SERIAL NOT NULL PRIMARY KEY,
  Customer_Name VARCHAR  NOT NULL CHECK (Customer_Name ~ '^([А-Я]{1}[а-я\- ]{0,}|[A-Z]{1}[a-z\- ]{0,})$'),
  Customer_Surname VARCHAR  NOT NULL CHECK (Customer_Surname ~ '^([А-Я]{1}[а-я\- ]{0,}|[A-Z]{1}[a-z\- ]{0,})$'),
  Customer_Primary_Email VARCHAR NOT NULL CHECK (Customer_Primary_Email ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'),
  Customer_Primary_Phone VARCHAR NOT NULL CHECK (Customer_Primary_Phone ~ '^\+7\d{10}$')
);

CREATE TABLE Cards(
  Customer_Card_ID BIGINT NOT NULL PRIMARY KEY,
  Customer_ID BIGINT NOT NULL,
  CONSTRAINT fk_Cards_PersonalInformation_Customer_ID FOREIGN KEY (Customer_ID) REFERENCES PersonalInformation(Customer_ID)
);

CREATE TABLE Transactions(
  Transaction_ID BIGINT PRIMARY KEY,
  Customer_Card_ID BIGINT NOT NULL,
	Transaction_Summ REAL NOT NULL,
	Transaction_DateTime TIMESTAMP(0)  WITHOUT TIME ZONE ,
	Transaction_Store_ID BIGINT NOT NULL,
  CONSTRAINT fk_Transactions_Cards_Customer_Card_ID FOREIGN KEY (Customer_Card_ID) REFERENCES Cards(Customer_Card_ID)
);

CREATE TABLE SKUGroup (
	Group_ID BIGINT PRIMARY KEY,
	Group_Name VARCHAR NOT NULL CHECK (Group_Name ~ '^[A-Za-zА-Яа-яЁё0-9_@!#%&()+-=*\s\[\]{};:''''"\\|,.<>?/`~^$]*$')
);

CREATE TABLE ProductGrid (
	SKU_ID BIGINT PRIMARY KEY,
	SKU_Name VARCHAR NOT NULL CHECK (SKU_Name ~ '^[A-Za-zА-Яа-яЁё0-9_@!#%&()+-=*\s\[\]{};:''''"\\|,.<>?/`~^$]*$'),
	Group_ID BIGINT NOT NULL,
	CONSTRAINT fk_ProductGrid_SKUGroup_Group_ID FOREIGN KEY (Group_ID) REFERENCES SKUGroup(Group_ID)
);

CREATE TABLE Checks(
  Transaction_ID BIGINT NOT NULL,
  SKU_ID BIGINT NOT NULL,
  SKU_Amount REAL NOT NULL,
  SKU_Summ REAL NOT NULL,
  SKU_Sum_Paid REAL NOT NULL,
  SKU_Discount REAL NOT NULL,
  CONSTRAINT fk_Checks_ProductGrid_SKU_ID FOREIGN KEY (SKU_ID) REFERENCES ProductGrid(SKU_ID),
  CONSTRAINT fk_Checks_Transactions_Transaction_ID FOREIGN KEY (Transaction_ID) REFERENCES Transactions(Transaction_ID)
);

CREATE TABLE Stores(
  Transaction_Store_ID BIGINT NOT NULL,
  SKU_ID BIGINT NOT NULL,
  SKU_Purchase_Price NUMERIC NOT NULL,
  SKU_Retail_Price NUMERIC NOT NULL,
  CONSTRAINT fk_Stores_ProductGrid_SKU_ID FOREIGN KEY (SKU_ID) REFERENCES ProductGrid(SKU_ID)
);

CREATE TABLE DateOfAnalysisFormation(
  Analysis_Formation TIMESTAMP(0)
);



--Procedure for import

CREATE OR REPLACE PROCEDURE import(tabl VARCHAR, filepath VARCHAR, delim VARCHAR(1) DEFAULT '\t')
AS $$
  BEGIN
    IF (delim = '\t') THEN
            EXECUTE concat('COPY ', tabl, ' FROM ''', filepath, ''' DELIMITER E''\t''', ' CSV;');
        ELSE
            EXECUTE concat('COPY ', tabl, ' FROM ''', filepath, ''' DELIMITER ''', delim, ''' CSV;');
        END IF;

  END;
  $$LANGUAGE plpgsql;
  
--Procedure for export

CREATE OR REPLACE PROCEDURE export (tabl VARCHAR, filepath VARCHAR, delim VARCHAR(1) DEFAULT '\t')
AS $$
  BEGIN
 IF (delim = '\t') THEN
            EXECUTE concat('COPY ', tabl, ' TO ''', filepath, ''' DELIMITER E''\t''', ' CSV;');
        ELSE
            EXECUTE concat('COPY ', tabl, ' TO ''', filepath, ''' DELIMITER ''', delim, ''' CSV;');
        END IF;
  END;
  $$LANGUAGE plpgsql;  



SET DATESTYLE to iso, DMY;  


-- //////////////// Tests /////////////////
-- Before call change the path to .tsv files

CALL import('PersonalInformation', '/Users/craftbec/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Personal_Data_Mini.tsv'); 
CALL import('Cards', '/Users/craftbec/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Cards_Mini.tsv'); 
CALL import('Transactions', '/Users/craftbec/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Transactions_Mini.tsv'); 
CALL import('SKUGroup', '/Users/craftbec/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Groups_SKU_Mini.tsv'); 
CALL import('ProductGrid', '/Users/craftbec/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/SKU_Mini.tsv'); 
CALL import('Checks', '/Users/craftbec/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Checks_Mini.tsv'); 
CALL import('Stores', '/Users/craftbec/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Stores_Mini.tsv');   
CALL import('DateOfAnalysisFormation', '/Users/craftbec/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/Date_Of_Analysis_Formation.tsv');

SELECT * FROM PersonalInformation;
SELECT * FROM Cards;
SELECT * FROM Transactions;
SELECT * FROM SKUGroup;
SELECT * FROM ProductGrid;
SELECT * FROM Checks;
SELECT * FROM Stores;
SELECT * FROM DateOfAnalysisFormation;

CALL export('PersonalInformation', '/Users/craftbec/Desktop/Personal_Data.tsv'); 
CALL export('PersonalInformation', '/Users/craftbec/Desktop/Personal_Data.csv', ','); 
CALL export('Cards', '/Users/craftbec/Desktop/Cards.tsv'); 
CALL export('Transactions', '/Users/craftbec/Desktop/Transactions.tsv'); 
CALL export('SKUGroup', '/Users/craftbec/Desktop/Groups_SKU.tsv'); 
CALL export('ProductGrid', '/Users/craftbec/Desktop/SKU.tsv'); 
CALL export('Checks', '/Users/craftbec/Desktop/Checks.tsv'); 
CALL export('Stores', '/Users/craftbec/Desktop/Stores.tsv');   
CALL export('DateOfAnalysisFormation', '/Users/craftbec/Desktop/Date_Of_Analysis_Formation.tsv');
