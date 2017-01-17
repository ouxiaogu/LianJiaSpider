import re
import urllib2
import sqlite3
import random
import threading
from bs4 import BeautifulSoup

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

lock = threading.Lock()

hds = [
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
]

class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):
        self.lock = threading.RLock() #锁
        self.path = path #数据库连接参数

        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)

    def get_conn(self):
        conn = sqlite3.connect(self.path)#,check_same_thread=False)
        conn.text_factory=str
        return conn

    def conn_close(self,conn=None):
        conn.close()

    def conn_trans(func):
        def connection(self,*args,**kwargs):
            self.lock.acquire()
            conn = self.get_conn()
            kwargs['conn'] = conn
            rs = func(self,*args,**kwargs)
            self.conn_close(conn)
            self.lock.release()
            return rs
        return connection

    @conn_trans
    def execute(self,command,method_flag=0,conn=None):
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except sqlite3.IntegrityError,e:
            #print e
            return -1
        except Exception, e:
            print e
            return -2
        return 0

    @conn_trans
    def fetchall(self,command="select name from xiaoqu",conn=None):
        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception,e:
            print e
            pass
        return lists

def gen_xiaoqu_insert_command(info_list):
    """
    生成小区数据库插入命令
    """
    items = ['?' for i in range(len(info_list))]
    item_placeholder = ','.join(items)
    t=tuple(info_list)
    command=(r"insert into xiaoqu values({})".format(item_placeholder),t)
    print command
    return command

def xiaoqu_spider(db_xq,url_page=u"http://sz.lianjia.com/xiaoqu/nantou/"):
    """
    爬取页面链接中的小区信息
    """
    try:
        num = random.randint(0,len(hds)-1)
        req = urllib2.Request(url_page,headers=hds[num])
        print num
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exit(-1)
    except Exception,e:
        print e
        exit(-1)

    #xiaoqu_list=soup.findAll('div',{'class':'info-panel'})
    xiaoqu_list=soup.findAll('li',{'class':'clear xiaoquListItem'})
    print len(xiaoqu_list)
    for xq in xiaoqu_list:
        #print xq.prettify()
        info_list=[]
        print xq.find('div', {'class':'title'}).find('a').text
        info_list.append(xq.find('div', {'class':'title'}).find('a').text)

        info = xq.find('div', {'class': 'info'})
        district = info.find('a', {'class': 'district'}).text
        bizcircle = info.find('a', {'class': 'bizcircle'}).text
        price_avg = xq.find('div', {'class': 'totalPrice'}).find('span').text

        tag = ''
        tags = info.find('div', {'class': 'tagList'})
        tagList = []
        for tag in tags.findAll('span'):
            tagList.append(tag.text)
        if len(tag)!= 0:
            tag = ';'.join(tagList)

        style, year = '', ''
        content = info.find('div', {'class': 'positionInfo'}).text.split()
        for c in content:
            if re.search(ur'[\u5E74]+',c): #年
                year = c
            elif re.search(ur'[\u677F|\u5854|\u697C]+',c): #板 塔
                style = c
        info_list += [district, bizcircle, style, year, price_avg, tag]

        command=gen_xiaoqu_insert_command(info_list)
        db_xq.execute(command,1)

def do_xiaoqu_spider(db_xq,district):
    """
    爬取大区域中的所有小区信息
    """
    url_header=u"http://sz.lianjia.com/xiaoqu/"
    url=url_header+u"rs%s/" % district
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=5).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        return
    except Exception,e:
        print e
        return

    d="d="+soup.find('div',{'class':'page-box house-lst-page-box'}).get('page-data')
    exec(d)
    total_pages=d['totalPage']

    threads=[]
    for i in range(total_pages):
        url_page=url_header+"/pg%drs%s/" % (i+1,district)
        t=threading.Thread(target=xiaoqu_spider,args=(db_xq,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print u"爬下了 %s 区全部的小区信息" % district

def gen_chengjiao_insert_command(info_list):
    """
    生成成交记录数据库插入命令
    """
    items = ['?' for i in range(len(info_list))]
    item_placeholder = ','.join(items)
    t=tuple(info_list)
    command=(r"insert into chengjiao values({})".format(item_placeholder),t)
    print command
    return command

def chengjiao_spider(db_cj,url_page=u"http://bj.lianjia.com/chengjiao/pg1rs%E5%86%A0%E5%BA%AD%E5%9B%AD"):
    """
    爬取页面链接中的成交记录
    """
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('chengjiao_spider',url_page)
        return
    except Exception,e:
        print e
        exception_write('chengjiao_spider',url_page)
        return

    cj_listContent = soup.find('ul',{'class':'listContent'})
    if cj_listContent is None:
        return
    cj_list=cj_listContent.findAll('li')
    for cj in cj_list:
        info_list=[]
        # href, name, style, area, orientation, decoration, elevator, dealDate, totalPrice, unitPrice, floor, year, estateType, school, subway
        href, name, style, area, orientation, decoration, elevator, dealDate, totalPrice, unitPrice, floor, year, estateType, school, subway = ['' for i in range(15)]
        content=cj.find('a')
        if not content:
            continue
        else:
            href=content['href']

        content=cj.find('div',{'class':'title'}).find('a').text.split()
        if content:
            name, style, area = content # 小区名称, 户型, 面积
        content=cj.find('div',{'class':'houseInfo'}).get_text()
        content=content.split('|')
        if content:
            if len(content) == 3:
                orientation, decoration, elevator = content
            elif len(content) == 2:
                orientation, decoration = content
        dealDate = cj.find('div',{'class':'dealDate'}).text
        totalPrice = cj.find('div',{'class':'totalPrice'}).find('span',{'class':'number'}).text
        unitPrice = cj.find('div',{'class':'unitPrice'}).find('span',{'class':'number'}).text

        content=cj.find('div',{'class':'positionInfo'}).get_text().split()
        if content:
            for c in content:
                c = unicode(c)
                if re.search(ur'[\u5C42]+',c): #u'层'
                    floor = c
                elif re.search(ur'[\u5E74]+',c): #年
                    year = c
        content=cj.find('div',{'class':'dealHouseInfo'})
        if content:
            content = content.find('span',{'class':'dealHouseTxt'})
            if content:
                content = content.findAll('span')
                for c in content:
                    c=unicode(c.text)
                    if re.search(ur'[\u6EE1]+', c):#满
                        estateType = c
                    elif re.search(ur'[\u5B66]+',c): #学
                        school = c
                    elif re.search(ur'[\u8DDD]+',c): #学
                        subway = c
        info_list = [href, name, style, area, orientation, decoration, elevator, dealDate, totalPrice, unitPrice, floor, year, estateType, school, subway]
        command=gen_chengjiao_insert_command(info_list)
        db_cj.execute(command,1)

def xiaoqu_chengjiao_spider(db_cj,xq_name=u"冠庭园"):
    """
    爬取小区成交记录
    """
    url_header = u"http://sz.lianjia.com/chengjiao/"
    url=url_header+u"rs"+urllib2.quote(xq_name)+"/"
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    except Exception,e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    content=soup.find('div',{'class':'page-box house-lst-page-box'})
    total_pages=0
    if content:
        d="d="+content.get('page-data')
        exec(d) #excute statements like 'd={"totalPage":36,"curPage":1}'
        total_pages=d['totalPage']

    threads=[]
    for i in range(total_pages):
        url_page=url_header+u"pg%drs%s/" % (i+1,urllib2.quote(xq_name))
        t=threading.Thread(target=chengjiao_spider,args=(db_cj,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

def do_xiaoqu_chengjiao_spider(db_xq,db_cj):
    """
    批量爬取小区成交记录
    """
    count=0
    xq_list=db_xq.fetchall()
    for xq in xq_list:
        xiaoqu_chengjiao_spider(db_cj,xq[0])
        count+=1
        print 'have spidered %d xiaoqu' % count
    print 'done'

def exception_write(fun_name,url):
    """
    写入异常信息到日志
    """
    lock.acquire()
    f = open('log.txt','a')
    line="%s %s\n" % (fun_name,url)
    f.write(line)
    f.close()
    lock.release()


def exception_read():
    """
    从日志中读取异常信息
    """
    lock.acquire()
    f=open('log.txt','r')
    lines=f.readlines()
    f.close()
    f=open('log.txt','w')
    f.truncate()
    f.close()
    lock.release()
    return lines


def exception_spider(db_cj):
    """
    重新爬取爬取异常的链接
    """
    count=0
    excep_list=exception_read()
    while excep_list:
        for excep in excep_list:
            excep=excep.strip()
            if excep=="":
                continue
            excep_name,url=excep.split(" ",1)
            if excep_name=="chengjiao_spider":
                chengjiao_spider(db_cj,url)
                count+=1
            elif excep_name=="xiaoqu_chengjiao_spider":
                xiaoqu_chengjiao_spider(db_cj,url)
                count+=1
            else:
                print "wrong format"
            print "have spidered %d exception url" % count
        excep_list=exception_read()
    print 'all done ^_^'

if __name__=="__main__":
    district = "新洲"
    command="create table if not exists xiaoqu (name TEXT primary key UNIQUE, district TEXT, bizcircle TEXT, style TEXT, year TEXT, price TEXT, tag TEXT)" #
    db_xq=SQLiteWraper('lianjia-xq-{}.db'.format(district),command)

    command="create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT, orientation TEXT, decoration TEXT, elevator TEXT, dealDate TEXT, totalPrice TEXT, unitPrice TEXT,floor TEXT, year TEXT, estateType TEXT, school TEXT, subway TEXT)"
    db_cj=SQLiteWraper('lianjia-cj-{}.db'.format(district),command)

    #爬下当前区域所有小区信息
    do_xiaoqu_spider(db_xq, district)

    #爬下所有小区里的成交信息
    do_xiaoqu_chengjiao_spider(db_xq,db_cj)

    #重新爬取爬取异常的链接
    exception_spider(db_cj)