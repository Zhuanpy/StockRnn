import mysql.connector

# 创建连接
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="651748264Zz*",
    database="stockpool"
)

# 创建游标对象
mycursor = mydb.cursor()

# 执行查询
mycursor.execute("SELECT * FROM poolcount")

# 获取查询结果
result = mycursor.fetchall()

# 遍历结果并打印
for row in result:
    print(row)
    print(type(row))
