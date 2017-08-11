-- MySQL dump 10.14  Distrib 5.5.52-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: canalesdb
-- ------------------------------------------------------
-- Server version	5.5.52-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Dumping data for table `explorer_query`
--
-- WHERE:  id = 1
SET @MAXID=(SELECT coalesce(max(id),0)+1 FROM explorer_query);
SET @NOW=(SELECT NOW());
SET @GENDATE=(SELECT DATE_FORMAT(@NOW, '%M %d, %Y at %H:%i:%s'));
SET @TABLEPAT='nwpat';
SET @TABLECAS='nwcas';
SET @TABLEPATCAS='nwpatcas';
#LOCK TABLES `explorer_query` WRITE;
/*!40000 ALTER TABLE `explorer_query` DISABLE KEYS */;
INSERT INTO `explorer_query` VALUES (@MAXID,'Patient_Record_Upload',CONCAT('SELECT\r\n	pc.case_number,\r\n	/* Req */ p.First_Name, \r\n	p.Middle_Name, \r\n	/* Req */p.Last_Name, p.Date_of_Birth as DOB,\r\n	/* Req */CASE p.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" then \"M\" ELSE \"U\" END AS Gender,\r\n	/* Req */p.Social_Security_Number as SSN, p.Street_1 as Address1, \r\n	p.Street_2 as Address2,\r\n	/* Req */p.City, p.State, p.Zip_Code as ZIP, \r\n	c.Marital_Status, p.Employment_Status, p.Chart_Number as Chart_No, p.Signature_on_File, \r\n	/* Req */coalesce(p.Phone_1, p.Contact_Phone_1) as Home_Phone, \r\n	p.Work_Phone, \'\' as Cell_Phone, p.EMail, p.Race, p.Ethnicity,\r\n	\r\n	\r\n	/* INSURANCE NUMBER 1 DATA  */\r\n	c.Insurance_Carrier_1 as HF_Payer_ID_P, \r\n	\'\' as Insurante_Type_P, \r\n	c.Group_Name_1 as Insurance_Group_Name_P,c.Group_Number_1 as Insurance_Group_Number_P, \r\n	c.Policy_Number_1 as Insurance_ID_Number_P, c.Copayment_Amount as CoPay_P, \r\n	c.Accept_Assignment_1 as Insured_Accept_Assingment_P, c.Insured_Relationship_1 as Patient_Relationship_P, \r\n	p1.First_Name as Insured_First_Name_P, p1.Middle_Initial as Insured_Middle_Initial_P, \r\n	p1.Last_Name as Insured_Last_Name_P, p1.Date_of_Birth as Insured_DOB_P, \r\n	CASE p1.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" then \"M\" ELSE \"U\" END AS Insured_Gender_P,\r\n	p1.Street_1 as Insured_Address1_P, p1.Street_2 as Insured_Address2_P, p1.City as Insured_City_P, \r\n	p1.State as Insured_State_P, p1.Zip_Code as Insured_Zip_P, coalesce(p1.Phone_1, p1.Contact_Phone_1) as Insured_Phone_P,\r\n	c.Prior_Authorization_No as Authorization_No_P, \r\n    \r\n    /* EMERGENCY CONTACT DATA */\r\n	left(p.Contact_Name, length(p.Contact_Name) - locate(\" \", reverse(p.Contact_Name))) as Emergency_First_Name, \r\n	SUBSTRING_INDEX(p.Contact_Name, \' \', -1) as Emergency_Last_Name, \r\n	COALESCE(p.Contact_Phone_1, p.Contact_Phone_2) as Emergency_Phone, \'Other\' as Emergency_Relation,\r\n	\r\n	/* INSURANCE NUMBER 2 DATA  */\r\n	c.Insurance_Carrier_2 as HF_Payer_ID_S, \r\n	\'\' as Insurante_Type_S, \r\n	c.Group_Name_2 as Insurance_Group_Name_S,c.Group_Number_2 as Insurance_Group_Number_S, \r\n	c.Policy_Number_2 as Insurance_ID_Number_S,  \r\n	\'\' as CoPay_S,  \r\n	c.Accept_Assignment_2 as Insured_Accept_Assingment_S, c.Insured_Relationship_2 as Patient_Relationship_S, \r\n	p2.First_Name as Insured_First_Name_S, p2.Middle_Initial as Insured_Middle_Initial_S, \r\n	p2.Last_Name as Insured_Last_Name_S, p2.Date_of_Birth as Insured_DOB_S, \r\n	CASE p2.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" then \"M\" ELSE \"U\" END AS Insured_Gender_S,\r\n	p2.Street_1 as Insured_Address1_S, p2.Street_2 as Insured_Address2_S, p2.City as Insured_City_S, \r\n	p2.State as Insured_State_S, p2.Zip_Code as Insured_Zip_S, coalesce(p2.Phone_1, p2.Contact_Phone_1) as Insured_Phone_S,\r\n	/*Falta*/ \'\'  as Authorization_No_S,\r\n	\r\n	/* INSURANCE NUMBER 3 DATA  */\r\n	c.Insurance_Carrier_3 as HF_Payer_ID_T, \r\n	/*Falta*/ \'\' as Insurante_Type_T, \r\n	c.Group_Name_3 as Insurance_Group_Name_T, c.Group_Number_3 as Insurance_Group_Number_T, \r\n	c.Policy_Number_3 as Insurance_ID_Number_T, \r\n	/*Falta*/ \'\' as CoPay_T, \r\n	c.Accept_Assignment_3 as Insured_Accept_Assingment_T, c.Insured_Relationship_3 as Patient_Relationship_T, \r\n	p3.First_Name as Insured_First_Name_T, p3.Middle_Initial as Insured_Middle_Initial_T, \r\n	p3.Last_Name as Insured_Last_Name_T, p3.Date_of_Birth as Insured_DOB_T, \r\n	CASE p3.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" then \"M\" ELSE \"U\" END AS Insured_Gender_T,\r\n	p3.Street_1 as Insured_Address1_T, p3.Street_2 as Insured_Address2_T, p3.City as Insured_City_T, \r\n	p3.State as Insured_State_T, p3.Zip_Code as Insured_Zip_T, coalesce(p3.Phone_1, p3.Contact_Phone_1) as Insured_Phone_T,\r\n	/*Falta*/ \'\'  as Authorization_No_T,\r\n	\r\n	/* - Guarantor - */\r\n	CASE when c.Guarantor = c.Chart_Number then \'Self\' when c.Guarantor = c.Insured_1 then c.Insured_Relationship_1 \r\n	END as Guarantor_Relation_To_Patient, g.First_Name as Guarantor_First_Name, g.Middle_Initial as Guarantor_Middle_Initial,\r\n	g.Last_Name as Guarantor_Last_Name, g.Date_of_Birth as Guarantor_DOB,\r\n	CASE g.Sex WHEN \"Female\" then \"F\" WHEN \"Male\" then \"M\" ELSE \"U\" END AS Guarantor_Gender,\r\n	g.Street_1 as Guarantor_Address1, g.Street_2 as Guarantor_Address2, g.City as Guarantor_City, g.State as Guarantor_State,\r\n	g.Zip_Code as Guarantor_ZIP, coalesce(g.Phone_1, g.Contact_Phone_1) as Guarantor_Home_Phone, g.Work_Phone as Guarantor_Work_Phone, \r\n	g.Social_Security_Number as Guarantor_SSN, \r\n	/* Guarantor\'s Employer Data */\r\n	g.Employer as Guarantor_Employer_Name, \'\' as Guarantor_Employer_Address1, \'\' as Guarantor_Employer_Address2, \r\n	\'\' as Guarantor_Employer_City, \'\' as Guarantor_Employer_State, \'\' as Guarantor_Employer_ZIP, g.Chart_Number as Guarantor_Account_Number, \r\n	g.Language, \r\n	/* NPI for Referring? */\r\n	c.Referring_Provider as Referring_Provider_NPI, \'\' as Location_External_ID\r\n	\r\nFROM ',@TABLEPAT,' p\r\nINNER JOIN (',@TABLEPATCAS,' pc \r\n			INNER JOIN (',@TABLECAS,' c\r\n						LEFT JOIN ',@TABLEPAT,' p1 on p1.Chart_Number = c.Insured_1\r\n						LEFT JOIN ',@TABLEPAT,' p2 on p2.Chart_Number = c.Insured_2\r\n						LEFT JOIN ',@TABLEPAT,' p3 on p3.Chart_Number = c.Insured_3\r\n						LEFT JOIN ',@TABLEPAT,' g on g.Chart_Number = c.Guarantor) \r\n			ON c.Chart_Number = pc.chart_number AND c.Case_Number = pc.case_number )\r\n ON pc.chart_number = p.Chart_Number\r\nORDER BY pc.case_number;'),concat('AutoGenerated on ',@GENDATE),@NOW,@NOW,1,0);
/*!40000 ALTER TABLE `explorer_query` ENABLE KEYS */;
#UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-08-10 13:19:34
--CREATE TABLE nwpatcas AS SELECT chart_number, MAX(case_number), count(*) as many FROM nwcas GROUP BY chart_number;