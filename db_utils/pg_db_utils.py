import psycopg2


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
            ping_res = self.cur.fetchall()
            print()
            print("connect pg success")
        except Exception as e:
            print("connect pg failed !",e)

    def close_db_client(self):
        self.cur.close()
        self.db.close()
        pass

    def query_all(self, sql):
        """
        查询sql
        :return:执行结果
        """
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        return rows

    # TODO 这个函数不知道什么作用,暂时保留
    def gettabledes_pg(self, table_schema, table_name):
        sql = "select row_number() over ()   as 序号,\
            t.column_name          as 字段名称,\
            pt.typname            as 字段类型,\
            case a.attnotnull when true then '是' else '否' end        as 允许空值\
            d.description         as 字段说明,\
            coalesce(character_maximum_length, numeric_precision, -1) as 字段长度,\
            from information_schema.columns t,\
            pg_attribute a,\
            pg_description d,\
            pg_class c,\
            pg_type pt,\
            pg_namespace pn\
            where d.objoid = a.attrelid\
                and d.objsubid = a.attnum\
                and a.attname = t.column_name\
                and a.attnum > 0\
                and a.atttypid = pt.oid\
                and a.attrelid = c.oid\
                and c.relnamespace = pn.""oid""\
                and c.relname = t.table_name\
                and pn.nspname = t.table_schema\
                and t.table_schema = '" + table_schema + "'\
                and t.table_name = '" + table_name + "';"
        # print(sql)
        list = []
        results = self.query_all(sql)
        # print(result)
        for row in results:
            list.append(row[1])
        print(self.getsqldict_pg(list, result))
        return self.getsqldict_pg(list, result)