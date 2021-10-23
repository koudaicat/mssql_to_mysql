# db_context.py
#encoding=utf-8
import pymysql
import datetime
import pymssql
import decimal
from dbutils.pooled_db import PooledDB
import time
import schedule
from loguru import logger
import json
import traceback
logger.add("record.log")

# 远程mysql


with open("./properties.json", "r", encoding="utf-8") as f:
    db = json.loads(f.read())
    print(db)

remote_ip = db["destination_db"]["host"]
remote_port = db["destination_db"]["port"]
remote_user = db["destination_db"]["user"]
remote_databases = db["destination_db"]["database"]
remote_pwd = db["destination_db"]["password"]
remote_device = db["destination_db"]["table"]

# 本地配置，需要修改
local_table = db["source_db"]["table"] # 表名称
# 本地mysql
local_ip = db["source_db"]["host"] #数据库ip
local_port = db["source_db"]["port"] #端口
local_user = db["source_db"]["user"] #用户
local_pwd = db["source_db"]["password"] #数据库密码
local_databases = db["source_db"]["database"] # 数据库名称
local_table_list = local_table.split(",")
table_key_info = dict()



POOL = PooledDB(
    creator=pymssql,  # 使用链接数据库的模块
    maxconnections=3,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=1,  # 链接池中最多闲置的链接，0和None不限制
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
    ping=0,
    host=local_ip,
    port=local_port,
    user=local_user,
    password=local_pwd,
    database=local_databases,
    charset="utf8"
)

POOL3 = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=5,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=3,  # 链接池中最多闲置的链接，0和None不限制
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
    ping=0,
    host=remote_ip,
    port=remote_port,
    user=remote_user,
    password=remote_pwd,
    database=remote_databases,
    charset='utf8'
)

def timer(func):
    def inner(*args, **kwargs):
        t1 = time.time()
        func(*args, **kwargs)
        t2 = time.time()
        print("time:",t2-t1)
        return func
    return inner


class Connect(object):
    def __init__(self, POOL):
        self.conn = conn = POOL.connection()
        # self.cursor = conn.cursor(pymysql.cursors.DictCursor)
        self.cursor = conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def exec(self, sql, **kwargs):
        self.cursor.execute(sql, kwargs)
        self.conn.commit()

    def execmany(self, sql, values):
        self.cursor.executemany(sql, values)
        self.conn.commit()


    def fetch_one(self, sql, **kwargs):
        self.cursor.execute(sql, kwargs)
        result = self.cursor.fetchone()
        return result

    def fetch_all(self, sql, **kwargs):
        self.cursor.execute(sql, kwargs)
        result = self.cursor.fetchall()
        return result

    def rollback_db(self):
        self.conn.rollback()

def translate_mssql(x):
    # print(x, str(x), type(x))
    if type(x) is datetime.datetime:
        # print("timetime")
        return str(x)
    # if type(x) == int:
    if x is None:
        return "0"
    elif type(x) == decimal.Decimal:
        return float(x)
    elif type(x) == str:
        print(x)
        print("be", x, x.encode("utf-8").decode("GBK"))

        # x = x.replace("ºÅÂ¥2Â¥", "楼")
        # x = x.replace("Â¥", "楼")
        print(x)
        return x
    else:
        return x




@timer
def transfer_data(old_id, table_name):
    with Connect(POOL) as f:
        keyword = table_key_info[table_name]
        sql = "select * from {} where {} > {}".format(table_name,keyword, old_id)
        res = f.fetch_all(sql, old_id=old_id)
        c_list = []
        tem_list = []
        num = 0
        if len(res) == 0:
            return 0
        for line in res:
            line = [ translate_mssql(x) for x in line]
            values = tuple(line)
            num += 1
            tem_list.append(str(values))
            if num % 1000 == 0:
                c_list.append(tem_list)
                tem_list = []
        c_list.append(tem_list)
    for manybox in c_list:
        manybox = ",".join(manybox)
        # print(manybox)
        sql = "insert into {} values {}".format(table_name, manybox)
        print("sql", sql)
        with Connect(POOL3) as fb:
            try:
                fb.exec(sql)
                print("success")
            except Exception as e:
                print(traceback.format_exc())

                logger.info(e)
                fb.rollback_db()
                break

def run(table_name):
    # for table_name in local_table_list:
    global table_key_info
    print(table_key_info)
    print("tablename", table_name)
    keyword = table_key_info[table_name]
    sql = "select {} from {}  order by {} desc limit 1".format(keyword, table_name, keyword)
    with Connect(POOL3) as f:
        r = f.fetch_one(sql)
        # print("S",r)
        if r == None:
            old_id = 0
        else:
            old_id = r[0]
        logger.info(old_id)
        transfer_data(old_id,table_name)

def get_table_info_from_mssql(table_name):
    sql = """
    SELECT
         表名       = Case When A.colorder=1 Then D.name Else '' End,
         表说明     = Case When A.colorder=1 Then isnull(F.value,'') Else '' End,
         字段序号   = A.colorder,
         字段名     = A.name,
         字段说明   = isnull(G.[value],''),
         标识       = Case When COLUMNPROPERTY( A.id,A.name,'IsIdentity')=1 Then '√'Else '' End,
         主键       = Case When exists(SELECT 1 FROM sysobjects Where xtype='PK' and parent_obj=A.id and name in (
                          SELECT name FROM sysindexes WHERE indid in( SELECT indid FROM sysindexkeys WHERE id = A.id AND colid=A.colid))) then '√' else '' end,
         类型       = B.name,
         占用字节数 = A.Length,
         长度       = COLUMNPROPERTY(A.id,A.name,'PRECISION'),
         小数位数   = isnull(COLUMNPROPERTY(A.id,A.name,'Scale'),0),
         允许空     = Case When A.isnullable=1 Then '√'Else '' End,
         默认值     = isnull(E.Text,'')
     FROM
         syscolumns A
     Left Join
         systypes B
     On
         A.xusertype=B.xusertype
     Inner Join
         sysobjects D
     On
         A.id=D.id  and D.xtype='U' and  D.name<>'dtproperties'
     Left Join
         syscomments E
     on
         A.cdefault=E.id
     Left Join
     sys.extended_properties  G
     on
         A.id=G.major_id and A.colid=G.minor_id
     Left Join

     sys.extended_properties F
     On
         D.id=F.major_id and F.minor_id=0
         where d.name='{}'    --如果只查询指定表,加上此条件
     Order By
         A.id,A.colorder""".format(table_name)
    with Connect(POOL) as f:
        r = f.fetch_all(sql)
        # print("R", r)
    return r

def from_mssql_get_create_sql(info):
    save_info = list()
    global table_key_info
    table_name = info[0][0]
    for line in info:
        info_json = dict()
        if line[6] == "v":
            table_key_info[table_name] = line[3]
        info_json["字段名"] = line[3]
        info_json["格式"] = line[7]
        info_json["字段长度"] = line[9]
        save_info.append(info_json)
    # t = tuple()
    tall= ""
    for i in save_info:
        tline = "{} {}({}),".format(i["字段名"], i["格式"], i["字段长度"])
        tall = tall + tline
    tall = "(" + tall[:-1] + ")"
    create_sql = """CREATE TABLE If Not Exists {} {}""".format(table_name, tall)
    return create_sql


def create_table_on_mysql(create_sql):
    with Connect(POOL3) as f:
        f.exec(create_sql)

def action():
    timenow = time.strftime("%Y%m", time.localtime())
    print(timenow)
    for table_name in local_table_list:
        print(table_name)
        if "DbDayxxxxx" in table_name:
            table_name = "DbDay" + str(timenow)
        print(table_name)
        infodb = get_table_info_from_mssql(table_name)
        create_sql = from_mssql_get_create_sql(infodb)
        print(create_sql)
        create_sql = create_sql.replace("datetime(23)", "varchar(30)")
        create_table_on_mysql(create_sql)
        run(table_name)


if __name__ == '__main__':
    # 根据mssql中的表一次创建mysql表
    # timenow = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    action()
    while True:
        try:
            schedule.every().day.at("20:30").do(action)
            while True:
                schedule.run_pending()
            time.sleep(30)
        except Exception as e:
            logger.info(e)

