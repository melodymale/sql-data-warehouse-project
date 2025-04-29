/*
=============================================================
Create Database
=============================================================
Script Purpose:
    This script drops and creates existing databases: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop databases ('bronze', 'silver', and 'gold') if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- drop databases if already existed
DROP DATABASE IF EXISTS bronze;
DROP DATABASE IF EXISTS silver;
DROP DATABASE IF EXISTS gold;


-- create new databases
CREATE DATABASE bronze;
CREATE DATABASE silver;
CREATE DATABASE gold;
