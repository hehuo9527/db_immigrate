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
    host = pg_configs["pg_db1"]["host"],
    port = pg_configs["pg_db1"]["port"],
    db = pg_configs["pg_db1"]["db"],
    username = pg_configs["pg_db1"]["username"],
    pwd = pg_configs["pg_db1"]["password"],
    )
    
    pg_table_name = "fv_clear"
    oracle_table_name = "FV_CLEAR"
    # 获取pg 表字段
    pg_cols_list=pg_client.get_table_schema(pg_table_name)


    #获取oracle 表定义
    oracle_cols_list=oracle_client.get_table_schema(oracle_table_name)
    for oracle_col in list(oracle_cols_list):
        filter_pg_cols_list=[ col for col in pg_cols_list if col.col_name == oracle_col[0].lower()]
        pg_cols_list=filter_pg_cols_list


    # 获取 oracle数据
    select_col=",".join(col.col_name for col in pg_cols_list)
    sql_query_oracle="select " + select_col + f" from {oracle_table_name}"
    oracle_data_list=oracle_client.query(sql_query_oracle)
    for row in oracle_data_list:
        for col, value in zip(pg_cols_list, row):
            if value is not None :
                col.col_default_value = value

    for pg_col in pg_cols_list:
        inser_sql=" insert into " + pg_table_name + " ( " + ",".join(final_oracle_cols_list) +" ) " + " values "  + " ( " + ",".join(final_data_list[0]) +" );"
        print("insert sql is --->",inser_sql)
        
    # 查找匹配数据
    # 若不允许为空则给默认值
    # 若string -> *
    # 若number -> 0
    # date  -> 20991231
    #    生成.txt文件
    # result = 'insert into ' + sqltablename + ' (' + table + ') ' + 'values (' + valueresult + '); '

