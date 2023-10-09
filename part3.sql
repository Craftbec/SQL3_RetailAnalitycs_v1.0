-- DROP ROLE IF EXISTS "Visitor";
-- DROP ROLE IF EXISTS "Administrator";
 
CREATE ROLE Administrator WITH
    LOGIN
    SUPERUSER
    CREATEDB
    CREATEROLE
    INHERIT
    REPLICATION
    CONNECTION LIMIT -1
    PASSWORD '123456';

CREATE ROLE Visitor WITH
    LOGIN
    NOSUPERUSER
    INHERIT
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION;

GRANT SELECT ON TABLE Cards TO Visitor;
GRANT SELECT ON TABLE Checks TO Visitor;
GRANT SELECT ON TABLE DateOfAnalysIsFormation TO Visitor;
GRANT SELECT ON TABLE PersonalInformation TO Visitor;
GRANT SELECT ON TABLE ProductGrid TO Visitor;
GRANT SELECT ON TABLE Segments TO Visitor;
GRANT SELECT ON TABLE SkuGroup TO Visitor;
GRANT SELECT ON TABLE Stores TO Visitor;
GRANT SELECT ON TABLE Transactions TO Visitor;


-- Test for Administrator role
SET ROLE Administrator;
SELECT * FROM SkuGroup;
INSERT INTO SkuGroup(group_id, group_name) VALUES (100, 'Стекло');
SELECT * FROM SkuGroup;
DELETE FROM SkuGroup WHERE group_id = 100;
SELECT * FROM SkuGroup;


-- Test for Visitor role
SET ROLE Visitor;
SELECT * FROM SkuGroup;
INSERT INTO SkuGroup(group_id, group_name) VALUES (100, 'Стекло');
SELECT * FROM SkuGroup;

-- Change on your role
DROP OWNED BY Visitor;
DROP ROLE IF EXISTS Visitor;
DROP OWNED BY Administrator;
DROP ROLE IF EXISTS Administrator;
SET ROLE <YourRole>
