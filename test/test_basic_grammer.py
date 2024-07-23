import pytest

from conf.db_config import *

from db_utils.oracle_db_utils import *
from db_utils.pg_db_utils import *


def test_db_connect():
    oracle_client = Oracle_client(
        host=oracle_configs["local"]["host"],
        port=oracle_configs["local"]["port"],
        db=oracle_configs["local"]["db"],
        username=oracle_configs["local"]["username"],
        pwd=oracle_configs["local"]["password"],
    )

    pg_client = Pg_client(
        port=5432,
        host="localhost",
        db="postgres",
        username="postgres",
        pwd="1234",
    )
    pg_table_name = "fv_clear"
    oracle_table_name = "fv_clear"
    # 获取pg 表字段
    pg_cols_list=pg_client.get_table_schema(pg_table_name)
    tar_get_cols=[]
    for pg_col in pg_cols_list:
        tar_get_cols.append(pg_col.name)

    #获取oracle 表定义
    oracle_cols_list=oracle_client.get_table_schema(oracle_table_name)
    for oracle_col in list(oracle_cols_list):
        if  oracle_col not in tar_get_cols:
            del oracle_cols_list[tar_get_cols]

    # 获取 oracle数据
    sql_query_oracle="select " + " ".join(oracle_cols_list) + " from fv_clear"
    oracle_data_list=oracle_client.query(sql_query_oracle)
    insert_table_name=""
    inser_sql=" insert into " + insert_table_name + " ( " + ",".join(oracle_cols_list) +" ) " + " values "  + " ( " + ",".join(oracle_data_list) +" );"
    print("insert sql is --->",inser_sql)
        
    # 查找匹配数据
    # 若不允许为空则给默认值
    # 若string -> *
    # 若number -> 0
    # date  -> 20991231
    #    生成.txt文件
    # result = 'insert into ' + sqltablename + ' (' + table + ') ' + 'values (' + valueresult + '); '

