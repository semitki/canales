# coding: utf-8
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
#Location = r'./canales/media/pictures/nwpat_EErd9Nc.csv'
#df = pd.read_csv(Location)
dwot = re.compile('\d{2,2}/\d{2,2}/\d{4,4}')
dwt = re.compile('\d{2,2}/\d{2,2}/\d{4,4} \d{1,2}:\d{2,2}:\d{2,2} (AM|PM)')
engine = create_engine('mysql+mysqldb://root:123asdqwe@127.0.0.1:3306/canalesdb')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

def read_csv(Location):
    return pd.read_csv(Location)

def str_or_date(val):
    if type(val) == str:
        if (dwot.match(val) or dwt.match(val)):
            return 'DATETIME'
        else:
            return ('VARCHAR')
    else:
        return 'VARCHAR(1200) CHARACTER SET utf8'

def process(df, t_name):
    t_cols = []
    for column in df:
        dt = type(df[column].iloc[0])
        if dt == str:
            c_type = df[column].apply(lambda x: str_or_date(x))[0]
            if c_type == 'VARCHAR':
                c_len = df[column].apply(lambda x: len(str(x))).max()
                c_def = ("%s(%i) CHARACTER SET utf8" % (c_type, c_len))
            else:
                c_def = c_type
            t_cols.append((column, c_def))
        elif dt == np.int64:
            t_cols.append((column, 'INT'))
        elif dt == float or np.float64:
            t_cols.append((column, 'FLOAT'))
        elif dt == np.bool_:
            t_cols.append((column, 'BOOL'))

    cont = 0
    create_table = ("CREATE TABLE %s (\n\r" % t_name)
    for c in t_cols:
        c_name = c[0].replace(' ', '_')
        if cont < len(t_cols)-1:
            create_table += '\t`'+c_name+'` '+c[1]+',\n\r'
        else:
            create_table += '\t`'+c_name+'` '+c[1]+'\n\r'
        cont+=1
    create_table +=');'

    # Dump SQL create table
    sql_file = open('/tmp/' + t_name + '.sql', 'w')
    sql_file.write(create_table)
    sql_file.close()
    # Store in DB
    session.execute(create_table)
    with engine.connect() as conn, conn.begin():
        df.to_sql(
            name=t_name,
            con=conn,
            flavor=None,
            schema=None,
            if_exists='append',
            index=False,
            index_label=None,
            chunksize=1000,
            dtype=None
        )


