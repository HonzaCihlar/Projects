USE master;
GO
-- CREATING DATABASE - Aby fungovalo drop databse, nesmí být aktivní
DROP DATABASE IF EXISTS MojeDatabaze;
CREATE DATABASE DWH;
USE DWH;

-- Creating schemas

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;