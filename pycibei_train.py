#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql

# mysql配置文件
config = {
    'host': 'rm-m5e7n2hx2bai082s2go.mysql.rds.aliyuncs.com',
    'port': 3306,
    'user': 'xibeidba',
    'passwd': 'xibei_DBA',
    'db':'train_data',
    'charset':'utf8',
    'cursorclass':pymysql.cursors.DictCursor
    }

# config = {
#     'host': 'rm-m5ew4zoeeig0aft95do.mysql.rds.aliyuncs.com',
#     'port': 3306,
#     'user': 'xibei_ljk',
#     'passwd': 'W2rL!79VUrUv^XD9',
#     'db':'cibei_data',
#     'charset':'utf8',
#     'cursorclass':pymysql.cursors.DictCursor
#     }

conn = pymysql.connect(**config)
conn.autocommit(1)
cursor = conn.cursor()

class checkdata:

    def __init__(self,conn,cursor,retail,product):
        self.conn = conn
        self.cursor = cursor
        self.retail = retail
        self.product = product

    def check_product(self):
        # 查询项目主文件
        self.cursor.execute('SELECt product_code,product_name,max_category_code,unit_code,buy_unit_code,part_unit_code FROM com_data_product where product_code=%s', (self.product))
        data_product=self.cursor.fetchall()
        return data_product

    def check_unit(self):
        # 查询项目主文件
        self.cursor.execute('SELECT product_code,unit_code_from,unit_code_to FROM com_data_unit_converse where product_code=%s', (self.product))
        data_product=self.cursor.fetchall()
        return data_product

    def check_org(self):
        # 查询门店组织
        self.cursor.execute("select code,name from com_data_org where CODE in (select parent_org_code from com_data_org_relation where type='FR'and child_org_code=%s)",  (self.retail))
        data_org=self.cursor.fetchall()
        return data_org
    
    def check_provider(self):
        # 查询供应商数据
        self.cursor.execute('select PRODUCT_CODE,RETAIL_CODE,provider_code,relation_status,priority from com_data_product_provider where  RETAIL_CODE=%s and PRODUCT_CODE= %s ',  (self.retail,self.product) )
        data=self.cursor.fetchall()
        return data

    def check_select(self,sql):
        # 查询通用函数
        self.cursor.execute(sql,(self.retail,self.product))
        rtdata=self.cursor.fetchall()
        return rtdata
    
    def check_delivery(self):
        # 查询配送商
        self.cursor.execute('select provider_code,product_code,store_code,relation_status,priority from com_data_product_provider where store_code in (select storeage_code from com_data_product_store_relation where  RETAIL_CODE=%s and  PRODUCT_CODE=%s) and product_code=%s',(self.retail,self.product,self.product))
        dvdata=self.cursor.fetchall()
        return dvdata

    def close(self):
        # 关闭游标连接
        self.cursor.close()
        # 关闭数据库连接
        self.conn.close()

if __name__ == '__main__':
    sql_list = ['select product_code,retail_code,relation_status,delivery_type from com_data_product_retial_relation where RETAIL_CODE=%s and PRODUCT_CODE=%s',
    'select product_code,retail_code,storeage_code,relation_status from com_data_product_store_relation where  RETAIL_CODE=%s and PRODUCT_CODE=%s ',
    "select product_code,retail_code,buy_tax_rate_code from com_data_product_tax where  RETAIL_CODE=%s and PRODUCT_CODE=%s and buy_tax_rate_code!='Y'",
    'select provider_code,price_group_id,price,unit_code,valid_date,invalid_date from com_data_retail_bug_price where price_group_id in  ( SELECT price_group_id from com_data_price_group_retails where  retail_code=%s) and PRODUCT_CODE=%s',
    'select provider_code,price,unit_code,valid_date,invalid_date from com_data_retail_bug_price where retail_code=%s and product_code=%s',
    "select provider_code,provider_name FROM com_data_provider where provider_code in (select PROVIDER_CODE from com_data_product_provider where  RETAIL_CODE=%s and  PRODUCT_CODE=%s and relation_status='Y' )",
    'select product_code,retail_code,provider_code,relation_status from com_data_product_provider where  RETAIL_CODE=%s and PRODUCT_CODE= %s ']
    print '---------------------------------'
    retail = input('please input retail code:')
    product = input('please input product code:')
    print u'您需要核验的门店编码为:',retail
    print u'您需要核验的物资编码为:' ,product
    print '---------------------------------'
    check = checkdata(conn,cursor,retail,product)
    product_data = check.check_product()
    if len(product_data):
        print u'您核验的物资存在有效:'
        for data in product_data:
            print u'物资编码：',data['product_code']
            print u'物资名称: ',data['product_name']
            print u'主计量单位',data['unit_code']
            print u'采购单位  ',data['buy_unit_code']
            print u'部件单位  ',data['part_unit_code']
            category_code = data['max_category_code']
            
    else:
        print u'您核验的物资不存在'
    print u'物资大类: ',category_code
    print '---------------------------------'
    unit_data= check.check_unit()
    if len(unit_data)  :
        print u'维护的计量单位转换关系如下:'
        for data in unit_data:
            print u'物资编码：', data['product_code']
            print u'转换前单位:',data['unit_code_from']
            print u'转换后单位:',data['unit_code_to']
    else:
        print u'木有维护计量单位转换关系'
    print '---------------------------------' 
    org_data = check.check_org()
    if len(org_data):
        print u'您核验的门店配置的法人公司如下:'
        for data in org_data:
            print u'法人公司编码：',data['code']
            print u'法人公司名称: ',data['name']
    else:
        print u'您核验的门店木有配置法人公司'
    print '---------------------------------'
    rel_data= check.check_select(sql_list[0])
    if len(rel_data) :
        print u'物资与门店关系正常'
        for data in rel_data:
            print u'物资配送类型为:' , data['delivery_type']
            print u'物资与门店关系状态:', data['relation_status']
        print '       -----------         ' 
        delivery_type = data['delivery_type']
        tax_data = check.check_select(sql_list[2])
        if len(tax_data) :
                print u'物资门店税率关系正常'
                for data in tax_data:
                    print u'税率码为:' ,data['buy_tax_rate_code']
        else:
                print u'木有配置税率'
        print '       -----------         ' 
        if delivery_type == 'ZC':
                print u'您核验的物资为自采物资，无需配置物资门店仓库关系'
        else:
            sto_data= check.check_select(sql_list[1])
            if len(sto_data) :
                print u'物资门店仓库关系正常'
                for data in sto_data:
                    print u'配送仓库为:', data['storeage_code']
                    print u'物资门店仓库关系状态为:', data['relation_status']
            else:
                print u'木有配置物资门店仓库关系'
    else:
        print u'木有配置物资与门店关系'
    print '---------------------------------'   
    pv_data = check.check_provider()   
    if len(pv_data) > 1 :
        print u'存在多个供应商:'
        for data in pv_data:
            print u'供应商编码：',data['provider_code'],
            print u'状态:', data['relation_status'],
            print u'优先级',data['priority']
    elif len(pv_data) == 1 :
        print u'只有一个供应商:'
        for data in pv_data:
            print u'供应商编码：' ,data['provider_code'],
            print u'状态:' ,data['relation_status'],
            print u'优先级',data['priority']
    else:
        print u'木有供应商请确认'
    print '---------------------------------' 
    if delivery_type =='ZC':
        zcpr_data = check.check_select(sql_list[4])
        if len(zcpr_data) > 1 :
            print u'存在多个价格:'
            for data in zcpr_data:
                print u'供应商编码：',data['provider_code'], u'价格:',data['price'], u'单位:',data['unit_code'], u'生效日期:',data['valid_date'], u'失效日期:',data['invalid_date']
        elif len(zcpr_data) == 1 :
            print u'只存在一个生效价格:'
            for data in zcpr_data:
                print u'供应商编码：',data['provider_code'], u'价格:',data['price'], u'单位:',data['unit_code'], u'生效日期:',data['valid_date'], u'失效日期:',data['invalid_date']
        else:
            print u'木有查询到价格'
    else:
        tppr_data = check.check_select(sql_list[3])
        if len(tppr_data) > 1 :
            print u'存在多个价格:'
            for data in tppr_data:
                print u'供应商编码：' + data['provider_code'],u'价格组为:',data['price_group_id'],u'价格:',data['price'],u'单位:',data['unit_code'],u'生效日期:',data['valid_date'],u'失效日期:',data['invalid_date']
        elif len(tppr_data) == 1 :
            print u'只有一个价格:'
            for data in tppr_data:
                print u'供应商编码：' + data['provider_code'],u'价格组为:',data['price_group_id'],u'价格:',data['price'],u'单位:',data['unit_code'],u'生效日期:',data['valid_date'],u'失效日期:',data['invalid_date']
        else:
            print u'木有查询到价格'
    print '---------------------------------' 
    if delivery_type =='ZS':
        dv_data = check.check_delivery()
        if len(dv_data) > 1 :
            print u'存在多个配送商:'
            for data in dv_data:
                print u'配送商编码：', data['provider_code'],u'仓库编码:',data['store_code'],u'状态:' , data['relation_status'],u'优先级:',data['priority']
        elif len(dv_data) == 1 :
            print u'只存在一个配送商:'
            for data in dv_data:
                print u'配送商编码：',data['provider_code'], u'仓库编码:' ,data['store_code'], u'状态:' ,data['relation_status'], u'优先级:',data['priority']
        else:
            print u'木有查询到配送商'        
    else:
        print u'您核验的物资无需验证配送商！'
    print '---------------------------------' 
    check.close()