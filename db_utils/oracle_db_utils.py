import cx_Oracle


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

    def query_all(self, sql):
        """
        查询sql
        :return:执行结果
        """
        sql = sql
        d1 = self.cur.execute(sql)  # 执行sql

        rows = d1.fetchall()
        return rows

    def getsqltatle_or(self):
        """
        获取对应表数据字段值
        :return:返回元组（表字段名，表字段属性）
        """
        des = self.cur.description  # 获取表描述结果为列表
        list_table_name = []  # 创建listi存放结果
        list_table_type = ""
        list_table_result = []
        # print(des)
        for i in des:
            stri = str(i)  # 将每个字段转换为字符串
            stri = (
                stri.replace("('", "")
                .replace(",", "")
                .replace("'", "")
                .replace(")", "")
            )  # 将字符串格式化
            stri = stri.split(" ")  # 通过空格切片字符串为列表
            # 列表第一列为表字段名
            stri1 = stri[0]  # 取列表第一个表字段值
            stri1 = stri1.lower()
            list_table_name.append(stri1)  # 将表名存入listi列表中
            # 列表第3列为数据类型
            stri3 = stri[2]
            # 列表第8列为是否可以为空，0为不能为空，1为可空
            stri8 = stri[7]
            list_table_type = ";".join([str(stri1), +str(stri3), str(stri8)])
            list_table_result.append(list_table_type)
        # print (listtablename)
        # print(listtableresult)
        return list_table_name, list_table_result

    def getsqldict(self, list_table, list_value):
        """
        :param listtable: 表字段名
        :param listvalue: 表字段值
        :return:字典列表：{’字段名‘：’字段值‘}
        """
        list_result = []
        dict = {}
        for a in list_table:
            dict[a] = ""
        for i in list_table:
            for j in range(0, len(list_table)):
                index = list_table.index(i)
                if index == j:
                    dict[i] = list_value[j]
                    break
                else:
                    pass
        list_result.append(dict)
        # print(listresult)
        return list_result
