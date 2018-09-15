#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql

#mysql配置文件
# config = {
#     'host': 'rm-m5e7n2hx2bai082s2go.mysql.rds.aliyuncs.com',
#     'port': 3306,
#     'user': 'xibeidba',
#     'passwd': 'xibei_DBA',
#     'db':'train_data',
#     'charset':'utf8',
#     'cursorclass':pymysql.cursors.DictCursor
#     }

config = {
    'host': 'rm-m5ew4zoeeig0aft95do.mysql.rds.aliyuncs.com',
    'port': 3306,
    'user': 'xibei_ljk',
    'passwd': 'W2rL!79VUrUv^XD9',
    'db':'cibei_data',
    'charset':'utf8',
    'cursorclass':pymysql.cursors.DictCursor
    }

conn = pymysql.connect(**config)
conn.autocommit(1)
cursor = conn.cursor()


def check_select(sql,retail):
    # 查询通用函数
    cursor.execute(sql,retail)
    rtdata=cursor.fetchall()
    return rtdata
    

# 关闭游标连接
cursor.close()
# 关闭数据库连接
conn.close()

if __name__ == '__main__':
    sql_list = [
        'select * from com_data_org where name like %s',
        'select * from com_data_product where product_code=%s'
    ]
    retail = input('please input retail code or product:')
    if len(retail) == 1:
        retail_data = check_select(sql_list[0],retail)
        print u'你输入的为门店编码:'
        for data in retail_data:
            print data
    elif len(retail) == 2:
        retail_data = check_select(sql_list[0],retail)
        print u'你输入的为物资编码:'
        for data in retail_data:
            print data  
    else:
        print u'请输入正确的物资或者门店编码'
        
