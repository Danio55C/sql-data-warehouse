/*
Create database and schemas

This scrip creeates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists it is dropped and recreated. Also script creates 3 schemas bronze, silver, and gold.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists. All data in the database will be permamently deleted. Proceed with caution.
*/


USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP TABLE DataWarehouse;
END;
GO


CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
