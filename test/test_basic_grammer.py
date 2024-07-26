import pytest

from conf.db_config import *

from db_utils.oracle_db_utils import *
from db_utils.pg_db_utils import *


def test_stru():
    col_data_list = {"key1": "value1", "key2": "value2", "key3": "value3"}
    cols_str = ",".join(k for k in col_data_list.keys())
    values = "','".join(v for v in col_data_list.values())
    values_str = f"'{values}'"
    inser_sql = (
        "insert into "
        + "pg_table_name"
        + " ("
        + cols_str
        + " ) vaules ("
        + values_str
        + " )"
    )
    print(inser_sql)
    with open("res.sql", "a") as file:
        file.write(inser_sql + "\n \n")


def test_db_connect():
    oracle_client = Oracle_client(
        host=oracle_configs["local"]["host"],
        port=oracle_configs["local"]["port"],
        db=oracle_configs["local"]["db"],
        username=oracle_configs["local"]["username"],
        pwd=oracle_configs["local"]["password"],
    )

    pg_client = Pg_client(
        host=pg_configs["pg_db1"]["host"],
        port=pg_configs["pg_db1"]["port"],
        db=pg_configs["pg_db1"]["db"],
        username=pg_configs["pg_db1"]["username"],
        pwd=pg_configs["pg_db1"]["password"],
    )

    pg_table_name = "fv_clear"
    oracle_table_name = "FV_CLEAR"
    # 获取pg 表字段
    pg_cols_list = pg_client.get_table_schema(pg_table_name)

    # 获取oracle 表定义
    oracle_cols_list = oracle_client.get_table_schema(oracle_table_name)
    for oracle_col in list(oracle_cols_list):
        filter_pg_cols_list = [
            col for col in pg_cols_list if col.col_name == oracle_col[0].lower()
        ]
        pg_cols_list = filter_pg_cols_list

    # 获取 oracle数据
    select_col = ",".join(col.col_name for col in pg_cols_list)
    sql_query_oracle = "select " + select_col + f" from {oracle_table_name}"
    oracle_data_list = oracle_client.query(sql_query_oracle)
    col_data_list = []
    for row in oracle_data_list:
        on_dict = {}
        for col, value in zip(pg_cols_list, row):
            on_dict[col.col_name] = col.col_default_value
            if value is not None:
                on_dict[col.col_name] = value
        col_data_list.append(on_dict)

    # 拼接sql
    for col_data_dict in col_data_list:
        cols_str = ",".join(k for k in col_data_dict.keys())
        values = "','".join(v for v in col_data_dict.values())
        values_str = f"'{values}'"
        inser_sql = (
            " insert into "
            + pg_table_name
            + " ("
            + cols_str
            + " ) vaules ("
            + values_str
            + " )"
        )
        print("insert sql is --->", inser_sql)
        with open("res.sql", "a") as file:
            file.write(inser_sql + "\n")

def test_test_pg_online():
     pg_client = Pg_client(
        host="ep-snowy-glitter-a1k95b2n.ap-southeast-1.aws.neon.tech",
        port=5432,
        db="pra",
        username="pra_owner",
        pwd="SMHQ0C9Ngrhn",
    )


def test_res():
    res={"a":"aaa","b":"bbbb","c":"cccc","d":"ddd","e":"eee","f":"sss"}
    manual_dict={ 
        # 需要替换值的字段 {拼接字符      [提供替换值的字段]}
        "a":{"connect_str":"","cols":["c","d"]},
        "b":{"connect_str":"@","cols":["e","f"]}
    }

    for k,connect_dict in manual_dict.items():
        replace_value=connect_dict["connect_str"].join(res[col] for col in connect_dict["cols"])
        res[k]=replace_value
    print(res)