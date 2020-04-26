import sys
import configparser
import os
import time
import pandas as pd
import re
from sqlalchemy import create_engine

def cfgParse(cfgParam):
    cfgParser = configparser.ConfigParser()
    cfgParser.read(cfgParam)
    section = 'default'
    dictionary = {}
    for section in cfgParser.sections():
        dictionary[section] = {}
        for option in cfgParser.options(section):
            dictionary[section][option] = cfgParser.get(section, option)
    return dictionary


def SourceFileCheck(source_list):
    print("Read Source File")
    frdr = pd.read_csv(source_list, header='infer', dtype=str)
    path = os.path.split(source_list)[0]
    InvalidFile = path + 'invalidfile' + '.txt'
    with open(InvalidFile, 'w') as wrtr:
        wrtr.write("source_schema,source_table,column,target_schema,target_table,status\n")
    dup = frdr.copy()
    clm = frdr.copy()
    dup['duplicate'] = dup.duplicated()
    dup = dup[dup['duplicate'] == True]
    dup.drop('duplicate', axis=1, inplace=True)
    dup['status'] = 'duplicate check'
    clm = clm[clm.isnull().any(axis=1)]
    clm['status'] = 'invalid record'
    dup.to_csv(InvalidFile, sep=',', header=False, mode='a', index=False)
    clm.to_csv(InvalidFile, sep=',', mode='a', header=False, index=False)
    frdr.drop_duplicates(inplace=True)
    frdr.dropna(inplace=True)
    return frdr


def connection(conn_file, conn_id):
    servername, database, port, username, password = dbutils(conn_file, conn_id)
    if re.search(r'_mysql\b', conn_id):
        db_connection_str = "mysql+pymysql://{2}:{3}@{0}/{1}".format(servername, database, username, password)
    elif re.search(r'_gpsql\b', conn_id):
        db_connection_str = "postgresql+psycopg2://{3}:{4}@{0}:{2}/{1}".format(servername, database, port, username,
                                                                               password)
    return db_connection_str


def tbl_count(confile, conn_id, tbl_hldr):
    db_connection_str = connection(confile, conn_id)
    query = "select '{0}' as table_name,count(1) as count from {1}".format(tbl_hldr.split('.')[-1], tbl_hldr)
    dbconn = create_engine(db_connection_str)
    try:
        frame = pd.read_sql(query, dbconn)
    except Exception as e:
        print("Exception Occurred :{}".format(str(e)))
    finally:
        dbconn.dispose()
    return frame


def unit_test_validation(project, subproject, db_src_id, db_tgt_id, conn_file, tgt_path, timestamp, ValidatedFile):
    print("Unit Testing Process Started")
    for id, row in ValidatedFile.iterrows():
        SrcHldr = row['source_schema'] + '.' + row['source_table']
        TgtHldr = row['target_schema'] + '.' + row['target_table']
        column = row['column']
        srcnt = tbl_count(conn_file, db_src_id, SrcHldr)
        tgtcnt = tbl_count(conn_file, db_tgt_id, TgtHldr)
        if int(srcnt['count'])>0 and int(tgtcnt['count'])>0 and(int(srcnt['count'])==int(tgtcnt['count'])):
            if re.search(r',',row['column']):
                column=row['column'].split(',')
            else:
                column=row['column']
            src_pcol= tblrcrd(conn_file,db_src_id,SrcHldr)
            tgt_pcol= tblrcrd(conn_file,db_tgt_id,TgtHldr)
            pcol_cnt = pcol_check(src_pcol,tgt_pcol,column)
            #data_check = data_validation(src_pcol,tgt_pcol)
        else:
            print("Source Count and target cout not matching for : ",str(TgtHldr))
            continue





def tblrcrd(conn_file,conn_id,hldr):
    query = "select * from {0} limit 10".format(hldr)
    db_connection_str=connection(conn_file,conn_id)
    dbconn = create_engine(db_connection_str)
    try:
        frame = pd.read_sql(query,dbconn)
    except Exception as e:
        print("Exception Occured : {}",str(e))
    finally:
        dbconn.dispose()
    return frame


def pcol_check(src_pcol,tgt_pcol,column):
    result=src_pcol[column]==tgt_pcol[column]
    result.all()
    return result.all()

def dbutils(conn_file, conn_id):
    with open(conn_file, 'r') as dbrdr:
        dbdtls = [c for c in dbrdr if conn_id in c]
        if len(dbdtls) == 0 :
            print("Db details not available for conn_id : ", conn_id)
            exit(1)
        else:
            servername = dbdtls[0].split('|')[1]
            port = dbdtls[0].split('|')[2]
            database = dbdtls[0].split('|')[3]
            username = dbdtls[0].split('|')[4]
            password = dbdtls[0].split('|')[5]
            return servername, database, port, username, password.strip('\n')


if __name__ == '__main__':
    cfgParam = 'unit_test_config.txt'
    cfgBuilder = cfgParse(cfgParam)
    project = cfgBuilder['default']["project"]
    subproject = cfgBuilder['default']["subproject"]
    db_src_id = cfgBuilder['default']["source_conn_id"]
    db_tgt_id = cfgBuilder['default']["target_conn_id"]
    conn_file = cfgBuilder['default']["credential_path"]
    tgt_path = cfgBuilder['default']["target_directory"]
    source_list = cfgBuilder['default']["source_table_list"]
    timestamp = time.strftime("%Y%m%d%H%M%S")

    ValidatedFile = SourceFileCheck(source_list)
    if ValidatedFile.empty == False:
        unit_test_validation(project, subproject, db_src_id, db_tgt_id, conn_file, tgt_path, timestamp, ValidatedFile)
    else:
        print("Source File Empty,Exiting The Process")
