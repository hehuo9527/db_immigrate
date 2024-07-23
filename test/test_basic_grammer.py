import pytest

from conf.db_config import *

from db_utils.oracle_db_utils import *
from db_utils.pg_db_utils import *


def test_db_connect():
    # oracle_client = Oracle_client(
    #     host=oracle_configs["local"]["host"],
    #     port=oracle_configs["local"]["port"],
    #     db=oracle_configs["local"]["db"],
    #     username=oracle_configs["local"]["username"],
    #     pwd=oracle_configs["local"]["password"],
    # )
    pg_client = Pg_client(
        port=5432,
        host="ep-snowy-glitter-a1k95b2n-pooler.ap-southeast-1.aws.neon.tech",
        db="pra",
        username="pra_owner",
        pwd="SMHQ0C9Ngrhn",
    )

    pg_table_name = "fv_clear"
    oracle_table_name = "fv_clear"
    # 获取pg 表字段
    pg_cols_list=pg_client.get_table_schema(pg_table_name)
    for pg_col in pg_cols_list:
        print(pg_col)
    #获取oracle 表定义
    # oracle_cols_list=oracle_client.get_table_schema(oracle_table_name)


    # 获取 oracle数据
    # oracle_client.query_all()
    # 查找匹配数据
    # 若不允许为空则给默认值
    # 若string -> *
    # 若number -> 0
    # date  -> 20991231
    #    生成.txt文件
    # result = 'insert into ' + sqltablename + ' (' + table + ') ' + 'values (' + valueresult + '); '

