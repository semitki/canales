# coding: utf-8
import os
import re
import csv
import traceback
import datetime
import MySQLdb as mdb
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+mysqldb://canalesuser:123asdqwezxc@localhost/canalesdb')
Session = sessionmaker()
Session.configure(bind=engine)


def close_insert(sql_str):
    return str(sql_str.replace('NULL,);', 'NULL);'))


def table_cas(t_name):
    ddl = """
    (
        `Item` INT,
        `Chart_Number` VARCHAR(8) CHARACTER SET utf8,
        `Case_Number` INT,
        `Description` VARCHAR(30) CHARACTER SET utf8,
        `Guarantor` VARCHAR(8) CHARACTER SET utf8,
        `Print_Patient_Statements` VARCHAR(5) CHARACTER SET utf8,
        `Marital_Status` VARCHAR(8) CHARACTER SET utf8,
        `Student_Status` VARCHAR(11) CHARACTER SET utf8,
        `Employment_Status` VARCHAR(12) CHARACTER SET utf8,
        `Employer` VARCHAR(5) CHARACTER SET utf8,
        `Employee_Location` INT,
        `Employee_Retirement_Date` INT,
        `Work_Phone` VARCHAR(13) CHARACTER SET utf8,
        `Work_Phone_Extension` INT,
        `Insured_1` VARCHAR(8) CHARACTER SET utf8,
        `Insured_Relationship_1` VARCHAR(6) CHARACTER SET utf8,
        `Insurance_Carrier_1` VARCHAR(5) CHARACTER SET utf8,
        `Accept_Assignment_1` VARCHAR(5) CHARACTER SET utf8,
        `Policy_Number_1` VARCHAR(15) CHARACTER SET utf8,
        `Group_Number_1` VARCHAR(16) CHARACTER SET utf8,
        `Percent_Covered_A_1` INT,
        `Percent_Covered_B_1` INT,
        `Percent_Covered_C_1` INT,
        `Percent_Covered_D_1` INT,
        `Percent_Covered_E_1` INT,
        `Percent_Covered_F_1` INT,
        `Percent_Covered_G_1` INT,
        `Percent_Covered_H_1` INT,
        `Policy_is_Capitated` VARCHAR(5) CHARACTER SET utf8,
        `Policy_1_Start_Date` VARCHAR(40) CHARACTER SET utf8,
        `Policy_1_End_Date` VARCHAR(40) CHARACTER SET utf8,
        `Copayment_Amount` INT,
        `Insured_2` VARCHAR(8) CHARACTER SET utf8,
        `Insured_Relationship_2` VARCHAR(6) CHARACTER SET utf8,
        `Insurance_Carrier_2` VARCHAR(5) CHARACTER SET utf8,
        `Accept_Assignment_2` VARCHAR(5) CHARACTER SET utf8,
        `Policy_Number_2` VARCHAR(15) CHARACTER SET utf8,
        `Group_Number_2` VARCHAR(14) CHARACTER SET utf8,
        `Percent_Covered_A_2` INT,
        `Percent_Covered_B_2` INT,
        `Percent_Covered_C_2` INT,
        `Percent_Covered_D_2` INT,
        `Percent_Covered_E_2` INT,
        `Percent_Covered_F_2` INT,
        `Percent_Covered_G_2` INT,
        `Percent_Covered_H_2` INT,
        `Policy_2_Start_Date` VARCHAR(40) CHARACTER SET utf8,
        `Policy_2_End_Date` VARCHAR(40) CHARACTER SET utf8,
        `Policy_2_Crossover_Claim` VARCHAR(5) CHARACTER SET utf8,
        `Insured_3` INT,
        `Insured_Relationship_3` VARCHAR(4) CHARACTER SET utf8,
        `Insurance_Carrier_3` INT,
        `Accept_Assignment_3` VARCHAR(5) CHARACTER SET utf8,
        `Policy_Number_3` INT,
        `Group_Number_3` INT,
        `Percent_Covered_A_3` INT,
        `Percent_Covered_B_3` INT,
        `Percent_Covered_C_3` INT,
        `Percent_Covered_D_3` INT,
        `Percent_Covered_E_3` INT,
        `Percent_Covered_F_3` INT,
        `Percent_Covered_G_3` INT,
        `Percent_Covered_H_3` INT,
        `Policy_3_Start_Date` INT,
        `Policy_3_End_Date` INT,
        `Facility` VARCHAR(5) CHARACTER SET utf8,
        `Related_to_Employment` VARCHAR(5) CHARACTER SET utf8,
        `Related_to_Accident` INT,
        `Nature_of_Accident` INT,
        `Same_or_Similar_Symptoms` VARCHAR(5) CHARACTER SET utf8,
        `Emergency` VARCHAR(5) CHARACTER SET utf8,
        `EPSDT` VARCHAR(5) CHARACTER SET utf8,
        `Family_Planning` VARCHAR(5) CHARACTER SET utf8,
        `Outside_Lab_Work` VARCHAR(5) CHARACTER SET utf8,
        `Lab_Charges` INT,
        `Date_of_Injury_Illness` INT,
        `Date_First_Consulted` INT,
        `Date_Unable_to_Work_From` INT,
        `Date_Unable_to_Work_To` INT,
        `Date_Tot_Disability_From` INT,
        `Date_Tot_Disability_To` INT,
        `Date_Part_Disability_From` INT,
        `Date_Part_Disability_To` INT,
        `Hospital_Date_From` INT,
        `Hospital_Date_To` INT,
        `Prior_Authorization_No` INT,
        `Death_Indicator` INT,
        `Illness_Indicator` INT,
        `Accident_State` INT,
        `Date_Similar_Symptoms` INT,
        `Medicaid_Resubmission_No` INT,
        `Medicaid_Original_Ref_No` INT,
        `Local_Use_A` VARCHAR(12) CHARACTER SET utf8,
        `Local_Use_B` INT,
        `Champus_Nonavailability` INT,
        `Champus_Branch_of_Service` INT,
        `Champus_Sponsor_Grade` INT,
        `Champus_Sponsor_Status` INT,
        `Champus_Special_Program` INT,
        `Champus_Card_Start_Date` INT,
        `Champus_Termination_Date` INT,
        `Return_to_Work_Indicator` INT,
        `Workers_Percent_Disabled` INT,
        `Diagnosis_1` INT,
        `Diagnosis_2` INT,
        `Diagnosis_3` INT,
        `Diagnosis_4` INT,
        `Last_Xray_Date` INT,
        `Level_of_Subluxation` INT,
        `EMC_Notes` VARCHAR(7) CHARACTER SET utf8,
        `Visit_Series_ID` VARCHAR(1) CHARACTER SET utf8,
        `Visit_Series_Counter` INT,
        `Last_Visit_Date` VARCHAR(40) CHARACTER SET utf8,
        `Authorized_No_of_Visits` INT,
        `Visit_Authorization_No` INT,
        `Treatment_Auth_Through` INT,
        `Attorney` INT,
        `Referring_Provider` VARCHAR(5) CHARACTER SET utf8,
        `Assigned_Provider` VARCHAR(2) CHARACTER SET utf8,
        `Referral_Source` INT,
        `Billing_Code` VARCHAR(1) CHARACTER SET utf8,
        `Price_Code` VARCHAR(1) CHARACTER SET utf8,
        `Indicator_1` INT,
        `Date_Created` VARCHAR(40) CHARACTER SET utf8,
        `Cash_Case` VARCHAR(5) CHARACTER SET utf8,
        `Case_Closed` VARCHAR(5) CHARACTER SET utf8,
        `Other_Arrangements` INT,
        `Extra_1` INT,
        `Extra_2` INT,
        `Extra_3` INT,
        `Extra_4` INT,
        `Primary_Care_Provider` INT,
        `Date_Last_Seen_PCP` INT,
        `Annual_Deductible` INT,
        `User_Code` VARCHAR(6) CHARACTER SET utf8,
        `Treatment_Months_Years` INT,
        `No_Treatments_Month` INT,
        `Nature_of_Condition` INT,
        `Date_of_Manifestation` INT,
        `Complication_Ind` INT,
        `Radiographs_enclosed` INT,
        `Prosthesis` VARCHAR(5) CHARACTER SET utf8,
        `Date_of_Prior_Placement` INT,
        `Reason_for_replacement` INT,
        `Orthodontics` VARCHAR(5) CHARACTER SET utf8,
        `Date_Treatment_Start` INT,
        `Date_Appliances_Placed` INT,
        `Length_of_Treatment` INT,
        `Medical_Plan_Coverage` VARCHAR(5) CHARACTER SET utf8,
        `Eligibility_Verified` INT,
        `Eligibility_Verified_Date` INT,
        `Eligibility_ID_Number` INT,
        `Eligibility_Verifier` INT,
        `Policy_Type` INT,
        `PC_Claim_Number_1` INT,
        `PC_Claim_Number_2` INT,
        `PC_Claim_Number_3` INT,
        `Referral_Date` INT,
        `Pregnancy_Indicator` VARCHAR(5) CHARACTER SET utf8,
        `Estimated_Date_of_Birth` INT,
        `Prescription_Date` INT,
        `Last_worked_Date` INT,
        `Date_assumed_care` INT,
        `Date_relinquished_care` INT,
        `Service_Authorization_Exception_Code` INT,
        `Report_type_code` INT,
        `Report_transmission_code` INT,
        `Homebound_indicator` VARCHAR(5) CHARACTER SET utf8,
        `IDE_Number` INT,
        `Supervising_Provider` VARCHAR(5) CHARACTER SET utf8,
        `Attachment_Control_Number` INT,
        `Deductible_Met` VARCHAR(5) CHARACTER SET utf8,
        `Notes` VARCHAR(85) CHARACTER SET utf8,
        `Date_Modified` VARCHAR(40) CHARACTER SET utf8,
        `Comment` VARCHAR(41) CHARACTER SET utf8,
        `Assignment_Indicator` INT,
        `Care_Plan_Oversight_Number` VARCHAR(10) CHARACTER SET utf8,
        `Hospice_Number` INT,
        `EPSDT_Code_1` VARCHAR(2) CHARACTER SET utf8,
        `EPSDT_Code_2` INT,
        `EPSDT_Code_3` INT,
        `Medicaid_Referral_Access_Number` INT,
        `Demonstration_Code` INT,
        `CLIA_Number` VARCHAR(10) CHARACTER SET utf8,
        `Mammography_Certification` INT,
        `Insurance_Type_Code` INT,
        `Timely_Filing_Indicator` INT,
        `Code_Category` INT,
        `Certification_Code_Applies` VARCHAR(5) CHARACTER SET utf8,
        `Condition_Indicator` INT,
        `Discipline_Type_Code` INT,
        `Total_Visits_Rendered` INT,
        `Total_Visits_Projected` INT,
        `Number_of_Visits` INT,
        `Frequency_Period` INT,
        `Frequency_Count` INT,
        `Duration` INT,
        `Number_of_Units` INT,
        `Pattern_Code` INT,
        `Time_Code` INT,
        `Diagnosis_Code5` INT,
        `Diagnosis_Code6` INT,
        `Diagnosis_Code7` INT,
        `Diagnosis_Code8` INT,
        `Diagnosis_Code1_POA` INT,
        `Diagnosis_Code2_POA` INT,
        `Diagnosis_Code3_POA` INT,
        `Diagnosis_Code4_POA` INT,
        `Diagnosis_Code5_POA` INT,
        `Diagnosis_Code6_POA` INT,
        `Diagnosis_Code7_POA` INT,
        `Diagnosis_Code8_POA` INT,
        `Operating_Provider` INT,
        `Other_Provider` INT,
        `Treatment_Authorization_63a` INT,
        `Treatment_Authorization_63b` INT,
        `Treatment_Authorization_63c` INT,
        `Primary_DCN_64a` INT,
        `Secondary_DCN_64b` INT,
        `Tertiary_DCN_64c` INT,
        `Insured_Relationship_Code_1` INT,
        `Insured_Relationship_Code_2` INT,
        `Insured_Relationship_Code_3` INT,
        `Global_Coverage_End_Date` INT,
        `Global_Coverage_Start_Date` INT,
        `Supervising_Provider_Type` VARCHAR(1) CHARACTER SET utf8,
        `Operating_Provider_Type` INT,
        `Other_Provider_Type` INT,
        `Group_Name_1` VARCHAR(38) CHARACTER SET utf8,
        `Group_Name_2` VARCHAR(24) CHARACTER SET utf8,
        `Group_Name_3` INT,
        `Special_Program_Code` INT,
        `Note_Reference_Code` INT,
        `Contract_Type_Code` INT,
        `Contract_Amount` INT,
        `Contract_Percent` INT,
        `Contract_Code` INT,
        `Terms_Discount_Percent` INT,
        `Contract_Version_Identifier` INT,
        `Condition_Description_1` INT,
        `Condition_Description_2` INT,
        `Diagnosis_Code9` INT,
        `Diagnosis_Code10` INT,
        `Diagnosis_Code11` INT,
        `Diagnosis_Code12` INT,
        `Diagnosis_Code9_POA` INT,
        `Diagnosis_Code10_POA` INT,
        `Diagnosis_Code11_POA` INT,
        `Diagnosis_Code12_POA` INT,
        `NUCC_Box8` INT,
        `NUCC_Box9b` INT,
        `NUCC_Box9c` INT,
        `NUCC_Box30` INT
    );
    """

    return str(('CREATE TABLE %s %s' % (t_name, ddl)))


def table_pat(t_name):

    ddl = """
    (
    `Item` INT,
    `Chart_Number` VARCHAR(8) CHARACTER SET utf8,
    `Last_Name` VARCHAR(18) CHARACTER SET utf8,
    `First_Name` VARCHAR(15) CHARACTER SET utf8,
    `Middle_Initial` VARCHAR(1) CHARACTER SET utf8,
    `Street_1` VARCHAR(30) CHARACTER SET utf8,
    `Street_2` VARCHAR(7) CHARACTER SET utf8,
    `City` VARCHAR(14) CHARACTER SET utf8,
    `State` VARCHAR(2) CHARACTER SET utf8,
    `Zip_Code` VARCHAR(10) CHARACTER SET utf8,
    `Phone_1` VARCHAR(13) CHARACTER SET utf8,
    `Phone_2` VARCHAR(13) CHARACTER SET utf8,
    `Phone_3` VARCHAR(13) CHARACTER SET utf8,
    `Phone_4` INT,
    `Phone_5` VARCHAR(13) CHARACTER SET utf8,
    `Social_Security_Number` VARCHAR(11) CHARACTER SET utf8,
    `Signature_on_File` VARCHAR(5) CHARACTER SET utf8,
    `Patient_Type` VARCHAR(9) CHARACTER SET utf8,
    `Patient_ID_2` INT,
    `Sex` VARCHAR(7) CHARACTER SET utf8,
    `Date_of_Birth` VARCHAR(40) CHARACTER SET utf8,
    `Assigned_Provider` VARCHAR(2) CHARACTER SET utf8,
    `Country` VARCHAR(7) CHARACTER SET utf8,
    `Date_of_Last_Payment` VARCHAR(40) CHARACTER SET utf8,
    `Last_Payment_Amount` NUMERIC(5, 2),
    `Patient_Reference_Balance` NUMERIC(6, 2),
    `Date_Created` VARCHAR(40) CHARACTER SET utf8,
    `Employment_Status` VARCHAR(12) CHARACTER SET utf8,
    `Employer` VARCHAR(5) CHARACTER SET utf8,
    `Employee_Location` INT,
    `Employee_Retirement_Date` INT,
    `Work_Phone` VARCHAR(13) CHARACTER SET utf8,
    `Work_Extension` INT,
    `SOF_Date` VARCHAR(40) CHARACTER SET utf8,
    `Billing_Code` VARCHAR(1) CHARACTER SET utf8,
    `Patient_Indicator` INT,
    `User_Code` VARCHAR(6) CHARACTER SET utf8,
    `Unique_Health_ID` INT,
    `EMail` VARCHAR(30) CHARACTER SET utf8,
    `Date_Modified` VARCHAR(40) CHARACTER SET utf8,
    `Contact_Phone_1` VARCHAR(13) CHARACTER SET utf8,
    `Contact_Phone_2` VARCHAR(13) CHARACTER SET utf8,
    `Contact_Name` VARCHAR(40) CHARACTER SET utf8,
    `Weight` INT,
    `Weight_Units` VARCHAR(2) CHARACTER SET utf8,
    `Flag` INT,
    `Inactive` VARCHAR(5) CHARACTER SET utf8,
    `In_Collections` VARCHAR(5) CHARACTER SET utf8,
    `Payment_Plan` INT,
    `Last_Patient_Payment_Date` VARCHAR(40) CHARACTER SET utf8,
    `Last_Patient_Payment_Amount` NUMERIC(5, 2),
    `Followed_Plan` VARCHAR(5) CHARACTER SET utf8,
    `EntityType` INT,
    `Patient_Remainder_Balance` NUMERIC(5, 2),
    `Middle_Name` VARCHAR(16) CHARACTER SET utf8,
    `Medical_Record_Number` VARCHAR(11) CHARACTER SET utf8,
    `Date_of_Death` INT,
    `Suffix` VARCHAR(1) CHARACTER SET utf8,
    `Race` VARCHAR(1) CHARACTER SET utf8,
    `Ethnicity` VARCHAR(1) CHARACTER SET utf8,
    `Language` VARCHAR(7) CHARACTER SET utf8
    );
    """
    return str(('CREATE TABLE %s %s' % (t_name, ddl)))


def process(csv_file, t_name):
    try:
        if t_name.find('nwcas') == 0:
            create_table = table_cas(t_name)
        else:
            create_table = table_pat(t_name)
        # Store in DB
        session = Session()
        session.execute(create_table)
        session.commit()
        session.close()

        ## Build INSERT statements
        inserts = []
        with open(csv_file) as csvfile:
            linereader = csv.reader(csvfile, delimiter='|', quotechar="'",
                                    quoting=csv.QUOTE_MINIMAL,
                                    skipinitialspace=True)
            headers = next(csvfile)
            for row in linereader:
                v_line = ','.join(row).replace(',,',',NULL,').replace("'",' ').replace(';',' ').replace('None','NULL').replace('"',"'").replace("''", 'NULL').replace('\\','')
                if (t_name.find('nwcas') == 0 and
                    len(v_line[:-1].split(',')) < 253):
                    v_line += 'NULL'

                inserts.append('INSERT INTO ' + str(t_name) +
                               ' VALUES (' + v_line + ');\r\n')

        # Dump SQL create table
        sql_file = open('/tmp/' + t_name + '.sql', mode='a')
        sql_file.write(create_table)
        for sql_str in inserts:
            sql_file.write(close_insert(sql_str))
        sql_file.close()

        conn = mdb.connect(
            host='localhost',
            user='canalesuser',
            passwd='123asdqwezxc',
            db='canalesdb'
        )
        conn.autocommit(True)
        cur = conn.cursor()
        for insert in inserts:
            sql_qry = close_insert(insert)
            try:
                cur.execute(sql_qry)
            except Exception:
                print(traceback.format_exc())
        cur.close()
        conn.close()

        f_type = ''
        if t_name.find('nwcas') == 0:
            f_type = 'cas'
        else:
            f_type = 'pat'

        return {
            'type':  f_type,
            'file': os.path.basename(csv_file),
            'TableName': t_name,
            'Status': 'Complete'
        }
    except Exception:
        print(traceback.format_exc())
        return {
            'Status': 'Error',
            'file': os.path.basename(csv_file)
        }
