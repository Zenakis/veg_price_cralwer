# -*- coding: utf8 -*-
import scrapy
import time
from scrapy.selector import Selector
from w3lib.html import remove_tags
import json
import os.path
import codecs
from kafka import KafkaProducer
from kafka.common import KafkaError

class VegtableProductJson(object):
    def __init__(self,transcation_date, marketList):
        self.transcation_date = transcation_date
        self.marketList = marketList
        
class Market(object):
    def __init__(self, market_id, market_name, vegtableProductPriceList):
        self.market_id = market_id
        self.market_name = market_name
        self.vegtableProductPriceList = vegtableProductPriceList
        
class VegtableProductPrice(object):
    def __init__(self, veg_id, veg_name, upper_price, median_price, lower_price, avg_price, trading_volum):
        self.veg_id = veg_id
        self.veg_name = veg_name
        self.upper_price = upper_price
        self.median_price = median_price
        self.lower_price = lower_price
        self.avg_price = avg_price
        self.trading_volum = trading_volum

class VegProdSpider(scrapy.Spider):
    name = 'vegetable-prod-spider'
    start_urls = ['http://amis.afa.gov.tw/veg/VegProdDayTransInfo.aspx']
    
    now_year = time.strftime("%Y-%m-%d", time.localtime()).split('-')[0]
    now_month =  time.strftime("%Y-%m-%d", time.localtime()).split('-')[1]
    now_day = time.strftime("%Y-%m-%d", time.localtime()).split('-')[2]
    tw_year = int(now_year) - 1911
    tw_date_format = str(tw_year)+'/'+now_month+'/'+now_day
    
    KAFKA_SERVER_IP = '59.127.187.54'
    KAFKA_SERVER_PORT = '9092'
    KAFKA_TOPIC = 'vegtable_product_price_test'
    
    UPDATE_FILE_LOG = '/home/webSpider/vegtable_price_crawler/update_time.txt'
    
    def parse(self, response):
        return scrapy.FormRequest.from_response(
            response,
            formdata={
                'ctl00$ScriptManager_Master':'ctl00$contentPlaceHolder$updatePanelMain|ctl00$contentPlaceHolder$btnQuery',
                'ctl00$contentPlaceHolder$ucSolarLunar$radlSolarLunar':'S',
                'ctl00$contentPlaceHolder$txtSTransDate':self.tw_date_format,
                'ctl00$contentPlaceHolder$txtETransDate':self.tw_date_format,
                'ctl00$contentPlaceHolder$txtMarket':u'全部市場',
                'ctl00$contentPlaceHolder$hfldMarketNo':'ALL',
                'ctl00$contentPlaceHolder$txtProduct':u'全部產品',
                'ctl00$contentPlaceHolder$hfldProductNo':'ALL',
                'ctl00$contentPlaceHolder$hfldProductType':'A',
#                '__EVENTTARGET':response.css('input#__EVENTTARGET::attr(value)').extract_first(),
#                '__EVENTARGUMENT':response.css('input#__EVENTARGUMENT::attr(value)').extract_first(),
#                '__VIEWSTATE':response.css('input#__VIEWSTATE::attr(value)').extract_first(),
#                '__VIEWSTATEGENERATOR':response.css('input#__VIEWSTATEGENERATOR::attr(value)').extract_first(),
#                '__EVENTVALIDATION':response.css('input#__EVENTVALIDATION::attr(value)').extract_first(),
#                '__ASYNCPOST':response.css('input#__ASYNCPOST::attr(value)').extract_first(),
#                'ctl00$contentPlaceHolder$btnQuery':u'查詢'
            },
            callback = self.after_query
        )
    
    def after_query(self, response):
        veg_data=[]
        transcation_date = ''
        for vegatable_price_table in response.css('div#ctl00_contentPlaceHolder_panel').extract():
            for parse_veg_data in  Selector(text=vegatable_price_table).xpath('//table[3]/tr/td'):
                veg_data.append(map(remove_tags, Selector(text=parse_veg_data.extract()).xpath('//text()').extract())) 
#                for parse_veg_td_data in Selector(text=parse_veg_data.extract()).xpath('//text()'):
#                    veg_data += parse_veg_td_data.extract()+";"
#                    print map(remove_tags, parse_veg_data.xpath('//text()').extract())

            for parse_data_date in Selector(text=vegatable_price_table).xpath('//table/tr/td/span[@id = \'ctl00_contentPlaceHolder_lblTransDate\']/text()'):    
    #                時間字串處理
                convert_date = parse_data_date.extract().split(" ")[0].split("/")
                convert_date[0] = str(int(parse_data_date.extract().split("~")[0].split("/")[0])+1911)
                convert_date_str = convert_date[0]+"/"+convert_date[1]+"/"+convert_date[2]+":00:00:00"
                transcation_date = time.mktime(time.strptime(convert_date_str.replace(" ",""), "%Y/%m/%d:%H:%M:%S"))
        if transcation_date != '':
#            self.check_update_time(veg_data, transcation_date)
            self.convert_vegtable_data(veg_data, transcation_date)
        else:
            print "No Data To Crawler"
            
    #確認資訊更新時間
    def check_update_time(self, veg_data, transcation_date):
        if os.path.exists(self.UPDATE_FILE_LOG):
            text_file = codecs.open(self.UPDATE_FILE_LOG, 'r', 'utf-8')
            if unicode(str(transcation_date),'utf-8') == unicode(text_file.readlines()[0].encode('utf-8'),'utf-8'):
                print "Data Uploaded"
            else:
                self.save_update_time(str(transcation_date))
                self.convert_vegtable_data(veg_data, transcation_date)
            text_file.close()
        else:
            self.save_update_time(str(transcation_date))
            self.convert_vegtable_data(veg_data, transcation_date)

    #儲存更新時間        
    def save_update_time(self,update_time):
        text_file = open(self.UPDATE_FILE_LOG,"w")
        text_file.write(update_time)
        text_file.close()
        print 'Update time saved.'
        
    def convert_vegtable_data(self, veg_data, transcation_date):    
        
#        刪除不ＴＡＢＬＥ不必要的資訊
        for index in range(17):
            del veg_data[0]

#        切割ＬＩＳＴ資訊
        vegtable_parse_list = list(self.chunks(veg_data, 9))
        
#        市場與行情的陣列
        marketList = []
        vegtableProductPriceList = []
        
#        市場分類紀錄
        market_Id_Reocrd=''
        market_Name_Record=''
        
        
#        解析行情訊息
        for vegtable_content_set in vegtable_parse_list:
            for index in range(len(vegtable_content_set)):
#===============市場
                if index==0:
                    for content in vegtable_content_set[index]:
                        text_list = content.split(' ')
                        for text_index in range(len(text_list)):
                            if text_index==0:
                                market_id = text_list[text_index]
#                                print "marketId " + marketId
                            elif text_index==1:
                                market_name = text_list[text_index]
#                                print "marketName " + marketName
#===============產品
                elif index==1:
                    for content in vegtable_content_set[index]:
                        text_list = content.split(' ')
                        veg_name = ''
                        for text_index in range(len(text_list)):
                            if text_list[text_index] != '':
                                if text_index==0:
                                    veg_id = text_list[text_index]
                                if text_index==1:
                                    veg_name = text_list[text_index]
                                else:
                                    veg_name += " " + text_list[text_index]
#                                print "veg_name " + veg_name
                                
#===============上價           
                elif index==2:
                    for content in vegtable_content_set[index]:
                        upper_price = content
#                        print "upper_price " + upper_price
#===============中價
                elif index==3:
                    for content in vegtable_content_set[index]:
                        median_price = content
#                        print "median_price " + median_price
#===============下價
                elif index==4:
                    for content in vegtable_content_set[index]:
                        lower_price = content
#                        print "lower_price " + lower_price
#===============平均價（元/公斤）
                elif index==5:
                    for content in vegtable_content_set[index]:
                        avg_price = content

#===============平均價跟前一交易日比較%（不記錄）
#                elif index==6:
#                    for content in vegtable_content_set[index]:
#===============交易量（公斤）
                elif index==7:
                    for content in vegtable_content_set[index]:
                        trading_volum = content
                
#===============交易量跟前一交易日比較%（不記錄）    
#                elif index==8:
#                    for content in vegtable_content_set[index]:
#                        text_list = content.split(' ')

#               資料分類
#                    print "market_Id_Reocrd : " +market_Id_Reocrd
#                    print "market_id : " +market_id
                    
                    
                    if market_Id_Reocrd != '':
                        if market_Id_Reocrd == market_id:
#                            print "market_id same"
                            vegtableProductPriceList.append(VegtableProductPrice(veg_id, veg_name, upper_price, median_price, lower_price, avg_price, trading_volum))
                            market_Id_Reocrd = market_id
                            market_Name_Record = market_name
                        else:
#                            print "market_package :" + market_Id_Reocrd
                            marketList.append(Market(market_Id_Reocrd, market_Name_Record, vegtableProductPriceList))
                            vegtableProductPriceList =[]
                            vegtableProductPriceList.append(VegtableProductPrice(veg_id, veg_name, upper_price, median_price, lower_price, avg_price, trading_volum))
                            market_Id_Reocrd = market_id
                            market_Name_Record = market_name
                    else:
#                        print "market init"
                        vegtableProductPriceList.append(VegtableProductPrice(veg_id, veg_name, upper_price, median_price, lower_price, avg_price, trading_volum))
                        market_Id_Reocrd = market_id
                        market_Name_Record = market_name
                    
#       最後市場的資料整合
        marketList.append(Market(market_Id_Reocrd, market_Name_Record, vegtableProductPriceList))
        
#        print "marketList " + str(len(marketList))
        self.sendMsgToKafka(VegtableProductJson(transcation_date, marketList))
    
#    def parse_to_json(self,vegtable_json_obj):
#
#        veg_price_json = json.dumps(vegtable_json_obj, default=self.jdefault , ensure_ascii=False, indent = 4)
#        print veg_price_json
        
    def sendMsgToKafka(self, vegtable_json_obj):
        producer = KafkaProducer(bootstrap_servers=[self.KAFKA_SERVER_IP + ":" + self.KAFKA_SERVER_PORT], value_serializer=lambda v: json.dumps(v,default=self.jdefault , ensure_ascii=False).encode('utf-8'))
        future = producer.send(self.KAFKA_TOPIC, vegtable_json_obj)
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            log.exception()
            pass
        
    def jdefault(self,o):
        if isinstance(o,set):
            return list(o)
        return o.__dict__
    
    def chunks(self, l, n):
        """Yield successive n-sized chunks from l."""
        for i in xrange(0, len(l), n):
            yield l[i:i+n]
            
