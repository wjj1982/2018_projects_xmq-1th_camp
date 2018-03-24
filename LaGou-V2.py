#-*-coding: utf-8 -*-
import urllib,time
import csv
import requests,threading
import Queue
import sys
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

    # 提取拉勾网python职位的信息主函数，用了requests post函数
    def download_file(self,url,data,header):
        positionName_Info = []
        education_Info = []
        city_Info = []
        financeStage_Info = []
        companyShortName_Info = []
        industryField_Info = []
        salary_Info = []
        district_Info = []
        positionAdvantage_Info = []
        companySize_Info = []
        workYear_Info = []

        # 判断url为空或者无法打开等异常情况
        if url is None:
            print 'no url'
            return None
        try:
            response = requests.post(url, params=data, headers=header)
        except Exception as e:
            print "open the url failed,error:{}".format(e)
            return None

        print 'get {} successfully!'.format(url)

        #返回json格式，然后逐级提取json数据
        response.encoding = 'utf-8'
        result_dict = response.json()
        result_dict2 = result_dict['content']['positionResult']['result']

        for job_info in result_dict2:
            #print job_info['salary']

            positionName_Info.append(job_info['positionName'])
            education_Info.append(job_info['education'])
            city_Info.append(job_info['city'])
            financeStage_Info.append(job_info['financeStage'])
            companyShortName_Info.append(job_info['companyShortName'])
            industryField_Info.append(job_info['industryField'])
            salary_Info.append(job_info['salary'])
            district_Info.append(job_info['district'])
            positionAdvantage_Info.append(job_info['positionAdvantage'])
            companySize_Info.append(job_info['companySize'])
            workYear_Info.append(job_info['workYear'])

        # 拉锁函数将单个列表组合成二维矩阵形式
        return zip(positionName_Info, education_Info, city_Info, financeStage_Info, companyShortName_Info,
                    industryField_Info,salary_Info, district_Info, positionAdvantage_Info, companySize_Info, workYear_Info)

    # 多线程主运行函数
    def run(self):
        while True:
            # 如果empty，退出线程循环
            if self.queue.empty():
                break
            else:
                #从queue队列逐个提取url，data和header
                url,data,header=self.queue.get()
                # 运行提取python职位主函数
                info_job=self.download_file(url,data,header)
                # 每个线程停两秒，目的是防止爬的太快，被反爬，但貌似没有效果，ps：停顿时间加在这个位置对否？
                time.sleep(2)
                self.queue.task_done()

                # 线程锁上场
                lock.acquire()
                try:
                    if info_job != None:#判断下python信息是否获取到，获取到后写csv文件。同时构造个字符串，供断点续传用，感觉有点low哈
                        self.write_each_row_in_csv(info_job)
                        self.write_url_in_txt(url+str(data))

                finally:
                    lock.release()

    # 定义python写csv函数
    def write_each_row_in_csv(self,job_text):
        with open('any.csv','ab') as wf:
            for i in job_text:
                writer = csv.writer(wf)
                writer.writerow(i)

    # 定义字符串写txt，供断点续传用
    def write_url_in_txt(self,url_text):
        with open('some_lagou.txt','ab') as wf:
            wf.write(url_text)

if __name__=='__main__':
    # 定义url列表（该列表里面是url，data，header组成的元组作为元素），后面遍历列表然后加入queue队列
    urls=[]
    url = 'https://www.lagou.com/jobs/positionAjax.json?needAddtionalResult=false&isSchoolJob=0'
    # user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Safari/537.36'
    header = {"Accept": "application/json, text/javascript, */*; q=0.01",
              "Accept-Encoding": "gzip, deflate, br",
              "Accept-Language": "zh-CN,zh;q=0.8",
              "Host": "www.lagou.com",
              "X-Requested-With": "XMLHttpRequest",
              "Origin": "https://www.lagou.com",
              "Referer": "https://www.lagou.com/jobs/list_python?labelWords=&fromSearch=true&suginput=",
              'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
              "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36"}
    for page_num in xrange(1,31):#查看到有30页进行循环，这个也有点low，没有自动判断有多少页
        if page_num == 1:#第一页和后面的页参数有点小区别，所以加个判断
            param = {'first': 'true',
                     'pn': page_num,
                     'kd': 'python'}
        else:
            param = {'first': 'false',
                     'pn': page_num,
                     'kd': 'python'}

        data = urllib.urlencode(param)
        url_list=url+str(data)#这个list作为断点续传判断依据

        # 断点续传判断，从txt中直接读出已经爬过的网页list，然后if判断下
        with open('some_lagou.txt','rb') as rf:
            reader=rf.read()
            # 对于没爬过的，直接加入urls中
            if url_list not in reader:
                print url_list
                urls.append((url,data,header))

    queue=Queue.Queue()
    threads=[]

    # urls遍历加入queue队列
    for url in urls:
        queue.put(url)

    # 定义两个线程同时爬，拉勾网貌似有反爬机制，5个网页后就不能爬了，只能过会再次执行
    for i in xrange(1,3):
        t=DownloadThread(queue)
        threads.append(t)
        t.start()
    for c in threads:
        c.join()





