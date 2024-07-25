import json

from conf.db_config import *

from db_utils.oracle_db_utils import *
from db_utils.pg_db_utils import *
import json, os,copy

def main():
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

    pg_table_name = "fv_asset_fund_real"
    if os.path.exists(f"{pg_table_name}.sql"):
        os.remove(f"{pg_table_name}.sql")
    oracle_table_name = "FV_ASSET_FUND"
    # 获取pg 表字段
    pg_cols_list = pg_client.get_table_schema(pg_table_name)

    # 获取oracle 表定义
    oracle_cols_list = oracle_client.get_table_schema(oracle_table_name)


    oracle_cols = []
    for oracle_col in oracle_cols_list:
        oracle_cols.append(oracle_col[0].lower())
    del_col_pg=[]

    origin_pg_cols_list=copy.deepcopy(pg_cols_list)
    for col in pg_cols_list:
        if col.col_name not in oracle_cols:
            del_col_pg.append(col)
    for del_col in del_col_pg:
        pg_cols_list.remove(del_col)

    # 获取 oracle数据
    select_col = " , \n ".join(col.col_name for col in pg_cols_list)
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

    # 补全dict
    final_origin_pg_cols_list=[]
    for col_data in col_data_list: #62
        for k,v in col_data.items(): #177
            tmp_all_cols=copy.deepcopy(origin_pg_cols_list)#178 [{obj},{obj2}  new Obje [Obj,Obj] -->sql  time==>  1 Obj-1 --->sql
            print("before-->",(json.dumps(tmp_all_cols)))
            for origin_pg_col in  tmp_all_cols:
                tmp_originpg_col=copy.deepcopy(origin_pg_col)
                if tmp_originpg_col.col_name==k:
                    tmp_originpg_col.col_default_value=v
            print("after-->",(json.dumps(tmp_all_cols)))
            final_origin_pg_cols_list.append(tmp_all_cols)
            return
    # 拼接sql
    for final_obj in final_origin_pg_cols_list:
        keys=[]
        values=[]
        for origin_pg_col_obj in final_obj:
            keys.append(origin_pg_col_obj.col_name)
            values.append(origin_pg_col_obj.col_default_value)

        cols_str = ",".join(k for k in keys)
        values = "','".join(str(v) for v in values)
        values_str = f"'{values}'"
        inser_sql = (
            " insert into "
            + pg_table_name
            + " ("
            + cols_str
            + " ) values ("
            + values_str
            + " );"
        )
        # print("insert sql is --->", inser_sql)
        with open(f"{pg_table_name}.sql", "a") as file:
            file.write(inser_sql + "\n")


if __name__ == "__main__":
    main()

    pass

