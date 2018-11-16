#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql
import datetime

# mysql配置文件
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

    def check_product_cat(self):
        # 查询物资分类
        self.cursor.execute("SELECt `level`,category_code,category_name from com_data_product_category where category_code in (select max_category_code  from com_data_product where product_code=%s UNION select mid_category_code from com_data_product where product_code=%s UNION select category_code from com_data_product where product_code=%s)", (self.product,self.product,self.product))
        cat_product=self.cursor.fetchall()
        return cat_product

    def check_unit(self):
        # 查询计量单位转换
        self.cursor.execute('select product_code,unit_code_from,unit_code_to FROM com_data_unit_converse where product_code=%s', (self.product))
        data_product=self.cursor.fetchall()
        return data_product

    def check_org(self):
        # 查询门店组织
        self.cursor.execute("select code,name from com_data_org where CODE in (select parent_org_code from com_data_org_relation where type='FR'and child_org_code=%s)",  (self.retail))
        data_org=self.cursor.fetchall()
        return data_org

    def check_delv(self):
        # 查询门店组织
        sq ='''
        SELECT RETAIL_NAME mdmc,PROVIDER_NAME gys,pc.CATEGORY_NAME wzlb,'每天' bfsj,CONCAT('T+',REQUIRE_TIME) bhgz,
        REFERENCE_DATE jzsj FROM com_data_delivery_cycles  dc,com_data_product_category pc  where 
        dc.CATEGORY_CODE=pc.category_code and 
        RETAIL_CODE=%s and REFERENCE_UNIT='EVERY_MONTH'
        group by RETAIL_NAME,PROVIDER_NAME,pc.CATEGORY_NAME,CONCAT('T+',REQUIRE_TIME) ,REFERENCE_DATE
        UNION
        SELECT RETAIL_NAME mdmc,PROVIDER_NAME gys,pc.CATEGORY_NAME wzlb,CONCAT('周',REFERENCE_POINT) bfsj,CONCAT('T+',REQUIRE_TIME) bhgz,
        REFERENCE_DATE jzsj FROM com_data_delivery_cycles  dc,com_data_product_category pc where 
        dc.CATEGORY_CODE=pc.category_code and 
        RETAIL_CODE=%s and REFERENCE_UNIT='EVERY_WEEK'
        group by RETAIL_NAME,PROVIDER_NAME,pc.CATEGORY_NAME,CONCAT('T+',REQUIRE_TIME),REFERENCE_POINT 
        '''
        self.cursor.execute(sq,(self.retail,self.retail))
        data_org=self.cursor.fetchall()
        return data_org
    
    def check_org_gs(self):
        # 查询门店基础信息
        self.cursor.execute("select code,name,account_number,area,distribution_company,rc.on_line_time from com_data_org dd , com_data_retail_config rc  where  dd.code=rc.retail_code and code=%s",  (self.retail))
        data_orgs=self.cursor.fetchall()
        return data_orgs

    def check_dept_sys(self):
        # 查询审核流
        sq = '''
        SELECT cd.name retail_name,dp.name dept_name,user_type_name 
FROM cibei_sys.sys_retail_audit_process ap,cibei_data.com_data_org cd,cibei_data.com_data_dept dp where 
ap.dept_code=dp.code and ap.org_code=dp.org_code and cd.code=ap.org_code and 
ap.org_code =%s
        '''
        self.cursor.execute( sq, (self.retail))
        data_orgs=self.cursor.fetchall()
        return data_orgs

    def check_sys(self):
        # 查询审核流
        sq = '''
            select ovn.org_name retail_name,ovn.value lttr,ovn.user_type_name 
lttr_name, pv.value nxtr,st.user_type_name nxtr_name from
cibei_data.com_data_parameter_value pv , cibei_sys.sys_retail_audit_type st ,
(
SELECT af.org_name,pv.value,rat.user_type_name,af.next_status FROM 
cibei_sys.sys_retail_audit_flow af ,cibei_data.com_data_parameter_value pv ,cibei_sys.sys_retail_audit_type rat
where 
pv.code=af.bill_status and rat.bill_status=af.bill_status  and 
af.bill_type='RETAIL_REQUIRE_ORDER'  and af.org_code=%s) ovn where ovn.next_status=pv.code and ovn.next_status=st.bill_status
        '''
        self.cursor.execute( sq, (self.retail))
        data_orgs=self.cursor.fetchall()
        return data_orgs

    def check_pd(self):
        # 查询盘点审核流
        sq = '''
        SELECT raf.org_name,bc.name,rat.user_type_name FROM  
cibei_sys.sys_retail_audit_flow raf,cibei_sys.sys_retail_audit_type rat,cibei_data.com_data_brand_copy bc
where rat.org_code=raf.org_code and rat.bill_status=raf.bill_status and rat.bill_type=raf.bill_type and 
bc.code=raf.bill_status and 
raf.next_status='E' and raf.bill_type='INV_RETAIL_CHECK' and raf.org_code =%s
        '''
        self.cursor.execute( sq, (self.retail))
        data_orgs=self.cursor.fetchall()
        return data_orgs


    def check_zb(self):
        # 查询门店支部
        self.cursor.execute("select code,name from com_data_org where code in (SELECT parent_org_code FROM `com_data_org_relation`  dr where  type='YY' and dr.child_org_code=%s )",  (self.retail))
        data_org=self.cursor.fetchall()
        return data_org

    def check_provider(self):
        # 查询供应商数据
        self.cursor.execute('SELECT pp.PRODUCT_CODE PRODUCT_CODE,pp.RETAIL_CODE RETAIL_CODE,pp.provider_code provider_code,dp.provider_name provider_name,pp.relation_status relation_status,pp.priority priority FROM com_data_product_provider pp LEFT JOIN com_data_provider dp ON dp.provider_code = pp.provider_code  WHERE RETAIL_CODE=%s and PRODUCT_CODE= %s ',  (self.retail,self.product) )
        data=self.cursor.fetchall()
        return data

    def check_select(self,sql):
        # 查询通用函数
        self.cursor.execute(sql,(self.retail,self.product))
        rtdata=self.cursor.fetchall()
        return rtdata
    
    def check_delivery(self):
        # 查询配送商
        self.cursor.execute('select pp.provider_code,dp.provider_name,pp.product_code,pp.store_code,pp.relation_status,pp.priority from com_data_product_provider pp left join com_data_provider dp on pp.provider_code=dp.provider_code where store_code in (select storeage_code from com_data_product_store_relation where  RETAIL_CODE=%s and  PRODUCT_CODE=%s) and product_code=%s',(self.retail,self.product,self.product))
        dvdata=self.cursor.fetchall()
        return dvdata
    
    def close(self):
        # 关闭游标连接
        self.cursor.close()
        # 关闭数据库连接
        self.conn.close()

def myAlign(string, length=0):
    	if length == 0:
		return string
	slen = len(string)
	re = string
	if isinstance(string, str):
		placeholder = ' '
	else:
		placeholder = u'　'
	while slen < length:
		re += placeholder
		slen += 1
	return re

if __name__ == '__main__':
    sql_list = ['select product_code,retail_code,relation_status,min_num,delivery_type from com_data_product_retial_relation where RETAIL_CODE=%s and PRODUCT_CODE=%s',
    'select product_code,retail_code,storeage_code,relation_status from com_data_product_store_relation where  RETAIL_CODE=%s and PRODUCT_CODE=%s ',
    "select product_code,retail_code,buy_tax_rate_code from com_data_product_tax where  RETAIL_CODE=%s and PRODUCT_CODE=%s and buy_tax_rate_code!='Y'",
    'select provider_code,price_group_id,price,unit_code,valid_date,invalid_date from com_data_retail_bug_price where price_group_id in  ( SELECT price_group_id from com_data_price_group_retails where  retail_code=%s) and PRODUCT_CODE=%s',
    'select provider_code,price,unit_code,valid_date,invalid_date from com_data_retail_bug_price where retail_code=%s and product_code=%s',
    "select provider_code,provider_name FROM com_data_provider where provider_code in (select PROVIDER_CODE from com_data_product_provider where  RETAIL_CODE=%s and  PRODUCT_CODE=%s and relation_status='Y' )",
    'select product_code,retail_code,provider_code,relation_status from com_data_product_provider where  RETAIL_CODE=%s and PRODUCT_CODE= %s ',
    'select code,name from com_data_storage where `code` in (SELECT storeage_code from com_data_product_store_relation where  retail_code=%s  and product_code=%s)',
    ]
    print '---------------------------------'
    retail = input('please input retail code:')
    product = input('please input product code:')
    # retail = '30016'
    # product = '22200179'
    check = checkdata(conn,cursor,retail,product)
    product_data = check.check_product()
    print u'您需要核验的门店编码为:',retail
    print u'您需要核验的物资编码为:' ,product
    print '---------------------------------'
    if len(product_data):
        print u'您核验的物资存在有效:'
        for data in product_data:
            print u'物资名称: ',data['product_name']
            print u'主计量单位',data['unit_code']
            print u'采购单位  ',data['buy_unit_code']
            print u'部件单位  ',data['part_unit_code']
            category_code = data['max_category_code']
    else:
        print u'您核验的物资不存在'
    print '---------------------------------'
    unit_data= check.check_unit()
    if len(unit_data)  :
        print u'维护的计量单位转换关系如下:'
        for data in unit_data:
            print u'转换前单位:',data['unit_code_from']
            print u'转换后单位:',data['unit_code_to']
    else:
        print u'木有维护计量单位转换关系'
    print '---------------------------------'
    cat_data = check.check_product_cat()
    for data in cat_data:
            print u'物资类别级别：',data['level']
            print u'物资类别编码: ',data['category_code']
            print u'物资类别名称: ',data['category_name']
            print '       -----------         '
    print '---------------------------------'
    orgs_data = check.check_org_gs()
    if len(orgs_data):
        print u'您核验的门店基础信息如下:'
        for data in orgs_data:
            print u'门店编码：',data['code']
            print u'门店名称: ',data['name']
            print u'帐套: ',data['account_number']
            print u'区域: ',data['area']
            print u'上传至: ',data['distribution_company']
            print u'上线时间: ',data['on_line_time']
    else:
        print u'您核验的门店木有配置法人公司'
    print '---------------------------------'
    retail_sys = check.check_sys()
    if len(retail_sys):
        print u'您核验的门店配置的审核流如下:'
        for data in retail_sys:
            print u'上一状态：',myAlign(data['lttr'],8),u'上一状态审核人: ',myAlign(data['lttr_name'],6),
            print u'下一状态: ',myAlign(data['nxtr'],8),u'下一状态审核人: ',myAlign(data['nxtr_name'],6)
    else:
        print u'您核验的门店配置的审核流为默认审核流'
    print '---------------------------------'
    retail_pd = check.check_pd()
    if len(retail_pd):
        print u'您核验的门店配置的盘点审核流如下:'
        for data in retail_pd:
            print u'盘点审核类型:',myAlign(data['name'],8),u'盘点审核人: ',myAlign(data['user_type_name'],6)
    else:
        print u'您核验的门店未配置盘点审核'
    print '---------------------------------'

    retail_dept_sys = check.check_dept_sys()
    if len(retail_dept_sys):
        print u'您核验的门店配置的档口审核流如下:'
        for data in retail_dept_sys:
            print u'档口名称:',myAlign(data['dept_name'],12),u'审核人: ',myAlign(data['user_type_name'])
    else:
        print u'您核验的门店配置的审核流为默认审核流'
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
    delv_data = check.check_delv()
    if len(delv_data):
        print u'您核验的门店配置的到货规则如下:'
        for data in delv_data:
            print u'门店名称：',myAlign(data['mdmc'],8),
            print u'供应商: ',myAlign(data['gys'],9),
            print u'物资类别: ',myAlign(data['wzlb'],7),
            print u'报货时间: ',myAlign(data['bfsj'],5),
            print u'报货规则：',myAlign(data['bhgz'],5),
            print u'截止时间: ',myAlign(data['jzsj'].strftime('%H:%M:%S'),9)
    else:
        print u'您核验的门店木有配置到货规则'
    print '---------------------------------'
    zb_data = check.check_zb()
    if len(zb_data):
        print u'您核验的门店支部信息如下:'
        for data in zb_data:
            print u'支部编码：',data['code']
            print u'支部名称: ',data['name']
    else:
        print u'您核验的门店木有爹'
    print '---------------------------------'
    rel_data= check.check_select(sql_list[0])
    if len(rel_data) :
        print u'物资与门店关系正常'
        for data in rel_data:
            print u'物资配送类型为:' , data['delivery_type']
            print u'物资起订量:' , data['min_num']
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
                    store_data = check.check_select(sql_list[7])
                print '       -----------         '
                if len(store_data):
                    for data in store_data:
                        print u'配送仓库名称为:',data['name']
                else:
                    print u'未配置仓库主数据'
                    print '---------------------------------'
            else:
                print u'木有配置物资门店仓库关系'
    else:
        print u'木有配置物资与门店关系'
    print '---------------------------------'
    pv_data = check.check_provider()   
    if len(pv_data) > 1 :
        print u'存在多个供应商:'
        for data in pv_data:
            print u'供应商编码：',data['provider_code'],u'供应商名字:',data['provider_name'],
            print u'状态:', data['relation_status'],
            print u'优先级',data['priority']
    elif len(pv_data) == 1 :
        print u'只有一个供应商:'
        for data in pv_data:
            print u'供应商编码：' ,data['provider_code'],u'供应商名字:',data['provider_name'],
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
                print u'配送商编码：', data['provider_code'],u'配送商名字:',data['provider_name'],u'仓库编码:',data['store_code'],u'状态:' , data['relation_status'],u'优先级:',data['priority']
        elif len(dv_data) == 1 :
            print u'只存在一个配送商:'
            for data in dv_data:
                print u'配送商编码：',data['provider_code'],u'配送商名字:',data['provider_name'],u'仓库编码:' ,data['store_code'], u'状态:' ,data['relation_status'], u'优先级:',data['priority']
        else:
            print u'木有查询到配送商'        
    else:
        print u'您核验的物资无需验证配送商！'
    print '---------------------------------' 
    check.close()