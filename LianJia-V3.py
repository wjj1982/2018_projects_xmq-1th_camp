#encoding=utf-8
import os,Queue,threading,requests
from bs4 import BeautifulSoup
import csv
import sys
import time
#reload下sys，然后设置下utf-8编码，这样才能爬取中文
reload(sys)
sys.setdefaultencoding('utf-8')

#启用个线程锁，后面csv写入时用到该锁，防止几个进程同时写一个文件会冲突
lock=threading.Lock()
#自定义一个线程类，将queue作为参数传入该类，queue就是url队列
class DownloadThread(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue=queue

    # 提取链家网南京二手房的信息主函数，用了requests和BeautifulSoup函数
    def download_file(self,url):
        # 判断url为空或者无法打开等异常情况
        if url is None:
            print 'no url'
            return None
        try:
            response=requests.get(url)
        except Exception as e:
            print "open the url failed,error:{}".format(e)
            return None
        if response.status_code != 200:
            print 'no 200'
            return None

        print 'get {} successfully!'.format(url)


        response=response.content
        bs1 = BeautifulSoup(response, 'html.parser')

        House_Inof = []
        Url_Info = []
        Position_Info = []
        Price_Ifo = []

        info_total = bs1.findAll('li', {'class': 'clear'})

        for row in info_total:
            info_house = row.find('div', {'class': 'houseInfo'})  # 筛选出houseInfo
            http_list = info_house.find('a')
            Url_Info.append(http_list.attrs['href'])  # ‘a’标签的第一个元素就是url网址，然后属性href可以直接过滤出url
            House_Inof.append(info_house.text)

            info_position = row.find('div', {'class': 'positionInfo'})  # 筛选出positonInfo
            Position_Info.append(info_position.text)

            info_price = row.find('div', {'class': 'priceInfo'})  # 筛选出priceInfo
            Price_Ifo.append(info_price.text)

        # 返回房屋信息，用于些csv文件用
        return zip(House_Inof, Position_Info, Price_Ifo, Url_Info)

    # 定义线程运行主函数
    def run(self):
        while True:
            # queue全部取完，变空时break，如果没有这个判断，就会一直循环
            if self.queue.empty():
                break
            # 从queue队列取url，然后主函数进行信息提取
            else:
                url=self.queue.get()
                info_house=self.download_file(url)
                self.queue.task_done() #提取信息后，队列任务结束弹出

                # 启用锁以及释放锁，中间是写csv文件函数
                lock.acquire()
                try:
                    # 判断渠道房屋信息后，进行写csv操作，同时将url写入txt，供断点续传判断用
                    if info_house != None:
                        self.write_each_row_in_csv(info_house)
                        self.write_url_in_txt(url)
                finally:
                    lock.release()

    # 写csv函数
    def write_each_row_in_csv(self,house_text):
        with open('some.csv','ab') as wf:
            for i in house_text:
                writer = csv.writer(wf)
                writer.writerow(i)

    # 写txt函数
    def write_url_in_txt(self,url_text):
        with open('some.txt','ab') as wf:
            wf.write(url_text)


if __name__=='__main__':
    urls=[]
    url_name = 'https://nj.lianjia.com/ershoufang/pg'
    # 链家网南京二手房共有100页
    for i in xrange(1,100):
        url_list = url_name + str(i) + '/'

        # 断点续传判断文件
        with open('some.txt','rb') as rf:
            reader=rf.read()
            # 没有爬过的url，加入queue队列
            if url_list not in reader:
                print url_list
                urls.append(url_list)

    queue=Queue.Queue()
    threads=[]

    for url in urls:
        queue.put(url)


    # 两个线程进行爬取
    for i in xrange(1,3):
        t=DownloadThread(queue)
        threads.append(t)
        # 这里停顿两秒，防止反爬，链家貌似没有反爬，这里加停顿对否？
        time.sleep(2)
        t.start()
    for c in threads:
        c.join()
