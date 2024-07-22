import pytest

from conf.db_config import *
from db_utils.oracle_db_utils import *
from db_utils.pg_db_utils import *




def test_db_connect():
    oracle_clien=Oracle_client(
        host=oracle_configs["local"]["host"],
        port=oracle_configs["local"]["port"],
        db=oracle_configs["local"]["db"],
        username=oracle_configs["local"]["username"],
        pwd=oracle_configs["local"]["password"],
    )
    pg_client=Pg_client(
        port=pg_configs["pg_db1"]["port"],
        host=pg_configs["pg_db1"]["host"],
        db=pg_configs["pg_db1"]["db"],
        username=pg_configs["pg_db1"]["username"],
        pwd=pg_configs["pg_db1"]["password"],
    )
    pg_table_name=""
    oracle_table_name=""
    #获取pg 表字段
    pg_client.query_all()
    #获取 oracle数据
    #查找匹配数据
    #若不允许为空则给默认值
        # 若string -> *
        # 若number -> 0
        # date  -> 20991231


#    生成.txt文件  
# result = 'insert into ' + sqltablename + ' (' + table + ') ' + 'values (' + valueresult + '); '

