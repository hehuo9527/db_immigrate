import psycopg2
from entity.schema import *
from typing import List

class Pg_client:

    def __init__(self, host, port, db, username, pwd):
        """
        数据库连接
        :param ip:数据库Ip
        :param port:数据库端口号
        :param db:
        :param username:用户名
        :param passworld:密码
        :return:
        """
        try:
            self.db = psycopg2.connect(
                host=host,
                port=port,
                database=db,
                user=username,
                password=pwd,
            )
            self.cur = self.db.cursor()
            self.cur.execute("SELECT version()")
            self.cur.fetchall()
            print()
            print("connect pg success !")
        except Exception as e:
            print("connect pg failed !", e)

    def close_db_client(self):
        self.cur.close()
        self.db.close()
        pass

    def get_table_schema(self, table_name) -> List[DB_Field]:
        query_sql = f"""select	a.attnum,	a.attname as field,	t.typname as type,	a.attnotnull as notnull,	a.attlen as length,	a.atttypmod as lengthvar
                        from  pg_class c,	pg_attribute a,	pg_type t 
                        where 	c.relname = '{table_name}'	and a.attnum > 0	and a.attrelid = c.oid	and a.atttypid = t.oid
                        order by 	a.attnum"""
        self.cur.execute(query_sql)
        list = self.cur.fetchall()
        cols = []
        for col in list:
            db_filed = DB_Field(col[1], col[2], col[3])
            cols.append(db_filed)
        return cols
