# coding: utf-8
import re
import csv
import traceback
import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


dwot = re.compile('\d{2,2}/\d{2,2}/\d{4,4}')
dwt = re.compile('\d{2,2}/\d{2,2}/\d{4,4} \d{1,2}:\d{2,2}:\d{2,2} (AM|PM)')
engine = create_engine('mysql+mysqldb://root:123asdqwe@127.0.0.1:3306/canalesdb')
Session = sessionmaker()
Session.configure(bind=engine)


def read_csv(csv_file):
    return pd.read_csv(openfile)


def str_or_date(val):
    if type(val) == str:
        #if (dwot.match(val) or dwt.match(val)):
        if dwot.match(val):
            return 'DATETIME'
        else:
            return ('VARCHAR')
    else:
        return 'VARCHAR(1200) CHARACTER SET utf8 DEFAULT NULL'


def close_insert(sql_str):
    return str(sql_str.replace('NULL,);', 'NULL);'))


def process(csv_file, t_name):
    try:
        ## Read CSV into a DataFrame
        df = pd.read_csv(csv_file)
        ## Iterate dataframe fields to infer SQL data types
        t_cols = []
        for column in df:
            dt = type(df[column].iloc[0])
            if dt == str:
                c_type = df[column].apply(lambda x: str_or_date(x))[0]
                if c_type == 'VARCHAR':
                    c_len = df[column].apply(lambda x: len(str(x))).max()
                    c_def = ("%s(%i) CHARACTER SET utf8 DEFAULT NULL" % (c_type, c_len))
                else:
                    c_def = c_type
                t_cols.append((column, c_def))
            elif dt == np.int64:
                t_cols.append((column, 'INT DEFAULT NULL'))
            elif dt == float or np.float64:
                t_cols.append((column, 'FLOAT DEFAULT NULL'))
            elif dt == np.bool_:
                t_cols.append((column, 'BOOL DEFAULT NULL'))

        ## Build the CREATE TABLE statement
        cont = 0
        insert_to = 'INSERT INTO '+t_name+' ('
        create_table = ("CREATE TABLE %s (\r\n" % t_name)
        for c in t_cols:
            c_name = c[0].replace(' ', '_')
            if cont < len(t_cols)-1:
                create_table += '\t`'+c_name+'` '+c[1]+',\n\r'
                insert_to += '`'+c_name+'`,'
            else:
                create_table += '\t`'+c_name+'` '+c[1]+'\n\r'
                insert_to += '`'+c_name+'`'
            cont+=1
        create_table += ');\r\n'
        insert_to += ')'

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
                    len(v_line[:-1].split(',')) < t_cols):
                    v_line += 'NULL'

                inserts.append('INSERT INTO ' + str(t_name) +
                               ' VALUES (' + v_line + ');\r\n')

        # Dump SQL create table
        sql_file = open('/tmp/' + t_name + '.sql', mode='a')
        sql_file.write(create_table)
        for sql_str in inserts:
            sql_file.write(close_insert(sql_str))
        sql_file.close()

        for insert in inserts:
            session = Session()
            sql_qry = close_insert(sql_str)
            try:
                session.execute(text(sql_qry))
                session.commit()
            except Exception:
                session.rollback()
                print(traceback.format_exc())
                print(sql_qry)
            finally:
                session.close()

        print("Finished!!!")
        return t_name
    except Exception:
        print(traceback.format_exc())
        return False
