#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql
import time
import sys


#mysql配置文件
# config = {
#     'host': 'rm-m5e7n2hx2bai082s2go.mysql.rds.aliyuncs.com',
#     'port': 3306,
#     'user': 'xibeidba',
#     'passwd': 'xibei_DBA',
#     'db':'train_sys',
#     'charset':'utf8',
#     'cursorclass':pymysql.cursors.DictCursor
#     }

config = {
    'host': 'rm-m5ew4zoeeig0aft95do.mysql.rds.aliyuncs.com',
    'port': 3306,
    'user': 'sys_mgr',
    'passwd': '2@u7r&Epxyf5S',
    'db':'cibei_data',
    'charset':'utf8',
    'cursorclass':pymysql.cursors.DictCursor
    }

conn = pymysql.connect(**config)
conn.autocommit(1)
cursor = conn.cursor()


def up_sys(sql,userlogin):
    # 执行SQL语句
    sta = cursor.execute(sql,userlogin)
    # 提交到数据库执行
    conn.commit();
    return (sta);
    
def close_base():
    # 关闭游标连接
    cursor.close()
    # 关闭数据库连接
    conn.close()

if __name__ == '__main__':
    sql_list = ['pass',
    "update `sys_user` set `password`='e10adc3949ba59abbe56e057f20f883e' where user_login = %s",
    "update sys_user_tparty  set is_active='N' where user_id in( select id  from sys_user where user_login=%s)",
    "update `sys_user` set is_active='N' where user_login = %s",
    "update `sys_user` set is_active='Y' where user_login = %s"
    ]
    while True:
        print u'======================================='
        print u'请选择你需要的使用功能，输入数字即可：'
        print u'1.重置密码'
        print u'2.解锁账号'
        print u'3.禁用账号'
        print u'4.启用账号'
        print u'5.退出程序'
        print u'======================================='
        values = input('please input your num: ')
        user   = input('please input user_login: ')
        values = int(values)
        print u'======================================='
        print u'你输入的账号为:',user
        print u'======================================='
        if   values == 1:
            try:
                data = up_sys(sql_list[2],user)
                if data == 1:
                    print u'重置密码成功!'
                elif data == 0:
                    print u'重置密码失败,可能是你输入的账号有误！请核查'
                else :
                    print u'重置密码可能出现了问题：',data
            except Exception as e:
	            #错误回滚
	            conn.rollback()

        elif values == 2:
            try:
                data = up_sys(sql_list[2],user)
                if data == 1:
                    print u'解锁账号成功,该账号只绑定了一个微信。'
                if data == 1:
                    print u'解锁账号成功,该账号绑定了',data,u'个微信。'
                elif data == 0:
                    print u'解锁账号失败,可能是你输入的账号有误！请核查'
                else :
                    print u'解锁账号：',data
            except Exception as e:
	            #错误回滚
	            conn.rollback()

        elif values == 3:
            try:
                data = up_sys(sql_list[3],user)
                if data == 1:
                    print u'禁用账号成功!'
                elif data == 0:
                    print u'禁用账号失败,可能是你输入的账号有误！请核查'
                else :
                    print u'禁用账号可能出现了问题：',data
            except Exception as e:
	            #错误回滚
	            conn.rollback()

        elif values == 520:
            print u'小姐姐，这是个隐藏的小彩蛋哦！'
            time.sleep(1)
            print u'     杨柳映春江'
            time.sleep(1)
            print u'     玄天幽且默'
            time.sleep(1)
            print u'     我辈复登临'
            time.sleep(1)
            print u'     喜元宵三五'
            time.sleep(1)
            print u'     欢饮仍如璧'
            time.sleep(1)
            print u'     你待更瞒咱'
            time.sleep(1)

        elif values == 4:
            try:
                data = up_sys(sql_list[4],user)
                if data == 1:
                    print u'启用账号成功!'
                elif data == 0:
                    print u'启用账号失败,可能是你输入的账号有误！请核查'
                else :
                    print u'启用账号可能出现了问题：',data
            except Exception as e:
	            #错误回滚
	            conn.rollback()

        elif values == 5 :
            print u'谢谢使用,欢迎再次使用！'
            break

        else:
            print u'小姐姐，别闹，叫你输入上面的数字,你输入其它的事要干啥？ 啊 哈'
            print u'======================================='
            print u'哈哈，叫你不听话，安安静静的等5秒钟吧'
            print u'======================================='
            time.sleep(1)
            print '55555'
            time.sleep(1)
            print '4444'
            time.sleep(1)
            print '333'
            time.sleep(1)
            print '22'
            time.sleep(1)
            print '1'
            print u'======================================='
            print u'哈哈，5秒过去了！别皮了，知道了吗？'
            continue
    #关闭数据库连接
    close_base()


