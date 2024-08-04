

from conf.db_config import *

from db_utils.oracle_db_utils import *
from db_utils.pg_db_utils import *
import  os,copy

def get_db_clients(oracle_db_name,pg_db_name)->tuple[Oracle_client,Pg_client]:
    oracle_client = Oracle_client(
        host=oracle_configs[oracle_db_name]["host"],
        port=oracle_configs[oracle_db_name]["port"],
        db=oracle_configs[oracle_db_name]["db"],
        username=oracle_configs[oracle_db_name]["username"],
        pwd=oracle_configs[oracle_db_name]["password"],
    )
    pg_client = Pg_client(
        host=pg_configs[pg_db_name]["host"],
        port=pg_configs[pg_db_name]["port"],
        db=pg_configs[pg_db_name]["db"],
        username=pg_configs[pg_db_name]["username"],
        pwd=pg_configs[pg_db_name]["password"],
    )
    return oracle_client,pg_client

def removeNotMatchCols(pg_cols_list,oracle_col_list):
    del_col_pg=[]
    for col in pg_cols_list:
        if col.col_name not in oracle_col_list:
            del_col_pg.append(col)

    for del_col in del_col_pg:
        pg_cols_list.remove(del_col)
    return pg_cols_list

def construct_col_data_dict(oracle_data_list,pg_cols_list):
    col_data_list = []
    for row in oracle_data_list:
        on_dict = {}
        for col, value in zip(pg_cols_list, row):
            on_dict[col.col_name] = col.col_default_value
            if value is not None:
                on_dict[col.col_name] = value
        col_data_list.append(on_dict)
    return col_data_list

def supplement_dict(part_col_data_list,origin_pg_cols_list):
    final_origin_pg_cols_list = []
    for col_data in part_col_data_list:
        tmp_all_cols = copy.deepcopy(origin_pg_cols_list)
        for k, v in col_data.items():
            for tmp_originpg_col in tmp_all_cols:
                if tmp_originpg_col.col_name == k:
                    tmp_originpg_col.col_default_value = v
        final_origin_pg_cols_list.append(tmp_all_cols)
    return final_origin_pg_cols_list

def main():
    oracle_client_db_name="local"
    oracle_table_name = "FV_ASSET_FUND"

    pg_client_db_name="pg_db1"
    pg_table_name = "fv_asset_fund_real"

    output_path = "E:\\sz\\实时估值测试\\博时POC\\POC数据\\脚本"

    manual_config= True
    manual_dict={ 
        # 需要替换值的字段 {拼接字符      [提供替换值的字段]}
        "col1":{"connect_str":"","cols":["col2","col3"]},
        "col2":{"connect_str":"@","cols":["col3","col3"]}
    }

    
    oracle_client,pg_client =get_db_clients(oracle_db_name=oracle_client_db_name,pg_db_name=pg_client_db_name)

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    if os.path.exists(f"{output_path}/{pg_table_name}.sql"):
        os.remove(f"{output_path}/{pg_table_name}.sql")

    # 获取pg 表字段
    pg_cols_list = pg_client.get_table_schema(pg_table_name)
    origin_pg_cols_list=copy.deepcopy(pg_cols_list)

    # 获取oracle 表定义
    oracle_cols = oracle_client.get_table_schema(oracle_table_name)
    pg_cols_list=removeNotMatchCols(pg_cols_list=pg_cols_list,oracle_col_list=oracle_cols)

    # 获取 oracle数据
    where_statment="ACCTING_UNIT='POC01'"
    select_col = " , \n ".join(col.col_name for col in pg_cols_list)
    sql_query_oracle = "select " + select_col + f" from {oracle_table_name}"
    if where_statment:
        sql_query_oracle+= "  where  "+where_statment
    oracle_data_list = oracle_client.query(sql_query_oracle)

    # col_data_list = []
    # for row in oracle_data_list:
    #     on_dict = {}
    #     for col, value in zip(pg_cols_list, row):
    #         on_dict[col.col_name] = col.col_default_value
    #         if value is not None:
    #             on_dict[col.col_name] = value
    #     col_data_list.append(on_dict)
    col_data_list=construct_col_data_dict(oracle_data_list=oracle_data_list,pg_cols_list=pg_cols_list)

    # 补全dict
    # final_origin_pg_cols_list = []
    # for col_data in col_data_list:
    #     tmp_all_cols = copy.deepcopy(origin_pg_cols_list)
    #     for k, v in col_data.items():
    #         for tmp_originpg_col in tmp_all_cols:``
    #             if tmp_originpg_col.col_name == k:
    #                 tmp_originpg_col.col_default_value = v
    #     final_origin_pg_cols_list.append(tmp_all_cols)
    final_origin_pg_cols_list=supplement_dict(part_col_data_list=col_data_list,origin_pg_cols_list=origin_pg_cols_list)


    if manual_config is True:
        for final_obj in final_origin_pg_cols_list:#final_obj [col1_obj,col2_obj,col3_obj,col4_obj]
            for replace_col,connect_dict in  manual_dict.items():
                    cols_value=[]
                    for col in connect_dict['cols']:
                        for obj in final_obj:
                            if obj.col_name==col:
                                cols_value.append(obj.col_default_value)
                    replace_value=connect_dict["connect_str"].join(cols_value)
                    final_ob[replace_col]=replace_value

    # 拼接sql 写入文件
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
        with open(f"{output_path}/{pg_table_name}.sql", "a") as file:
            file.write(inser_sql + "\n")


if __name__ == "__main__":
    # main()
    manual_dict={ 
        # 需要替换值的字段 {拼接字符      [提供替换值的字段]}
        "col1":{"connect_str":"","cols":["a","e"]},
        "col2":{"connect_str":"@","cols":["b","f"]}
    }
    final_ob={"a":"aaa","b":"bbbb","c":"cccc","d":"ddd","e":"eee","f":"sss"}
    for k,v in manual_dict.items():
        replace_value=v["connect_str"].join(final_ob[col] for col in v["cols"])
        print(replace_value)
    pass

