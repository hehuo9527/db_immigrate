import cx_Oracle
from entity.schema import *

class Oracle_client:

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
            tns = cx_Oracle.makedsn(host, port, db)  # 监听Oracle数据库

            self.db = cx_Oracle.connect(username, pwd, tns)  # 连接数据库

            self.cur = self.db.cursor()  # 创建游标

            self.cur.execute("select * from v$version")
            print()
            print("connect oracle success")
        except Exception as e:
            print("connect oracle failed !",e)

    def closrcur_client(self):
        """
        关闭游标
        :return:
        """
        self.cur.close()  # 关闭游标
        self.db.close()  # 关闭数据库

    def query_all(self, table_name):
        """
        查询sql
        :return:执行结果
        """
        sql = f"select * from {table_name}"
        d1 = self.cur.execute(sql)  # 执行sql
        rows = d1.fetchall()
        return rows

    def get_table_schema(self, table_name):
        query_sql = f"""select	a.attnum,	a.attname as field,	t.typname as type,	a.attnotnull as notnull,	a.attlen as length,	a.atttypmod as lengthvar
                        from  pg_class c,	pg_attribute a,	pg_type t 
                        where 	c.relname = '{table_name}'	and a.attnum > 0	and a.attrelid = c.oid	and a.atttypid = t.oid
                        order by 	a.attnum"""
        self.cur.execute(query_sql)
        list = self.cur.fetchall()
        cols=[]
        for col in list:
            db_filed=DB_Field(col[1],col[2],col[3])
            cols.append(db_filed)
        return cols


