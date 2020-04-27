import scrapy
import logging
import base64
from scrapy.selector import Selector
from time import sleep
from sqlalchemy import create_engine
import pandas as pd
from pymysql import *

class QuotesSpider(scrapy.Spider):
    name = "quotes"

    # def check_ip(self):
    #     url = 'http://guba.eastmoney.com/list,000001_%d.html'
    #     engine = create_engine('mysql://wy:,.,.,l@10.24.224.249/webdata?charset=utf8')
    #     self.df_proxy = pd.read_sql('select ip from Proxy', engine)
    #     self.valid_ip = []
    #     for i, ip_proxy in enumerate(self.df_proxy['ip']):
    #         scrapy.Request(url=url % (i+1), callback=self.parse1, meta={'proxy':'https://wangxwang:898990@%s' % ip_proxy})



    def start_requests(self):
        conn = connect(host='10.24.224.249', port=3306, database='webdata', user='wy',password=',.,.,l', charset='utf8')
        self.cur = conn.cursor()
        self.engine = create_engine('mysql://wy:,.,.,l@10.24.224.249/webdata?charset=utf8')
        self.df_proxy = pd.read_sql('select ip from Proxy', self.engine)

        url_temp = 'http://guba.eastmoney.com/list,000001_%d.html'
        urls =[url_temp % (i+1) for i in range(len(self.df_proxy))]

        for url,ip_proxy in zip(urls,self.df_proxy['ip'].values):
            yield scrapy.Request(url,callback=self.parse, meta={'proxy':'https://wangxwang:898990@%s' % ip_proxy})

    def parse(self, response):
        code = Selector(text=response.text).xpath('//*[@id="stockname"]/a/@href').extract_first().split(',')[1]
        idx = int(response.url.split('_')[1].split('.')[0])-1
        logging.warning('=====================')
        logging.warning(code)

        if code == '000001.html':
            logging.warning(self.df_proxy['ip'].values[idx])
            # self.valid_ip.append(self.df_proxy['ip'].values[idx])
            query = "update Proxy set score=1 where ip=\'%s\'" % (self.df_proxy['ip'].values[idx])
            self.cur.execute(query)

        # page = response.url.split(".")[-2]
        # filename = '%s.html' % page
        # ip = Selector(text=response.text).xpath('//*[@id="stockname"]/a').extract_first()
        # print(response.url)
        pass
        #with open(filename, 'wb') as f:
        #    f.write(response.body)
        #self.log('Saved file %s' % filename)
