# -*- coding: utf-8 -*-
# python36
__author__ = 'wuqili'

# -*- coding: utf-8 -*-
# python36
__author__ = 'wuqili'
# 抓取商品的评论，

import matplotlib
matplotlib.use('Agg')
import requests
import re
import json
import queue
import sys
import os
import numpy as np
from PIL import Image
from threading import Lock, Thread, currentThread
from scrapy.selector import Selector
from urllib.parse import urlencode, urlparse
from snownlp import SnowNLP
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import mogu_ip
import base64
from io import BytesIO
import matplotlib.pyplot as plt
from urllib.error import URLError
from wordcloud import WordCloud
import jieba

getip = mogu_ip.GetIP()

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'}
lock = Lock()
requests.adapters.DEFAULT_RETRIES = 5

comm_count = {}


class CommentParse:
    def __init__(self):
        self.comm = {}
        self.urlQueue = queue.Queue()
        self.scores = np.empty(0)
        self.ids = []
        self.use_proxy = False

    def get_comments(self):
        raise NotImplementedError

    def build_queue(self):
        raise NotImplementedError

    def get_all_comments(self):
        """使用多线程抓取评论页"""
        # 当当网要重写这个函数
        self.build_queue()
        if self.urlQueue.qsize() > 10:
            self.use_proxy = True
        if not self.urlQueue.empty():
            threads = []
            # 可以调节线程数， 进而控制抓取速度
            thread_num = self.urlQueue.qsize() // 3 + 1
            if thread_num >= 15:
                thread_num = 15
            if thread_num <= 3:
                thread_num = 3
            for i in range(thread_num):
                t = Thread(target=self.get_comments)
                threads.append(t)
            for t in threads:
                t.start()
            for t in threads:
                # 多线程多join的情况下，依次执行各线程的join方法, 这样可以确保主线程最后退出， 且各个线程间没有阻塞
                t.join()
        else:
            raise URLError('商品链接地址不能被识别')

    def comments_scores(self):
        """对评论进行文本分析"""
        for key, value in self.comm.items():
            self.ids.append(key)
            self.scores = np.append(self.scores, value['scores'])
        # 平均值
        mean_score = self.scores.mean().round(2)
        if mean_score in self.scores:
            mean_comm = self.comm[self.ids[int(
                np.argwhere(self.scores == mean_score)[0])]]
        else:
            sorted_scores = sorted(np.append(self.scores, mean_score))
            index = int(np.argwhere(sorted_scores == mean_score))
            near_score = sorted_scores[index + 1]
            mean_comm = self.comm[self.ids[int(
                np.argwhere(self.scores == near_score)[0])]]
        # 最大最小值
        min_score = self.scores.min()
        max_score = self.scores.max()
        min_comm = self.comm[self.ids[self.scores.argmin()]]
        max_comm = self.comm[self.ids[self.scores.argmax()]]
        return mean_score, min_score, max_score, mean_comm, min_comm, max_comm

    def hist_scores(self):
        """对所有评分进行直方图分布分析"""
        fig = plt.figure(figsize=(6, 6))
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 显示中文标签
        plt.hist(
            self.scores,
            bins=20,
            rwidth=0.8,
            facecolor='blue',
            alpha=0.5,
            edgecolor='yellow')
        plt.title('评论打分分布图', fontsize=20)
        plt.xlabel('评论分数', fontsize=16)
        plt.ylabel('评论计数', fontsize=16)
        plt.xticks(fontsize=14)
        plt.yticks(fontsize=14)
        sio = BytesIO()
        plt.savefig(sio, format='png')
        hist_data = "data:image/png;base64," + base64.encodebytes(sio.getvalue()).decode()
        sio.close()
        plt.close()
        return hist_data

    def show_part_scores(self):
        # 分出好评，中评和差评
        good_comments = []
        mid_comments = []
        bad_comments = []
        for key, value in self.comm.items():
            if 70 < value['scores'] <= 100:
                good_comments.append(value)
            elif 40 < value['scores'] <= 70:
                mid_comments.append(value)
            else:
                bad_comments.append(value)
        return good_comments, mid_comments, bad_comments

    def build_wordcloud(self, comments, img_model):
        """对评论内容生成词云"""
        text = ''
        for item in comments:
            text += item['content']
        if not text:
            words = '评论为空'
        else:
            wordlist = jieba.cut(text)
            words = ' '.join(wordlist)
        graph = Image.open(img_model)
        image = np.array(graph)
        wc = WordCloud(
            background_color='white',
            font_path=os.path.join(os.path.dirname(__file__), 'simsun.ttf'),
            mask=image,
            max_words=100)
        wc.generate(words)
        lock.acquire()
        plt.imshow(wc, interpolation='bilinear')
        plt.axis('off')
        wcio = BytesIO()
        plt.savefig(wcio, format='png')
        plt.close()
        lock.release()
        #将图片数据转化成base64
        wc_data = "data:image/png;base64," + base64.encodebytes(wcio.getvalue()).decode()
        wcio.close()
        return wc_data


class JDcomments(CommentParse):
    """获取京东商品的评论"""
    def __init__(self, product_url, timeId):
        super().__init__()
        self.product_url = product_url
        self.pub_time = set()
        self.timeId = timeId

    def build_queue(self):
        """将所有需要爬取的url放入到队列中"""
        # 商品id
        dept, id = re.findall(r'.*?jd\.(\w+?)/(\d+).*', self.product_url)[0]
        if dept == 'hk':
            # 京东全球购
            comm_url = 'https://club.jd.com/productpage/p-{0}-s-0-t-1-p-{1}.html'
        elif dept == 'com':
            # 普通京东购物
            comm_url = 'https://sclub.jd.com/comment/productPageComments.action?productId={0}&score=0&sortType=5&page={1}&pageSize=10'
        else:
            return
        # 爬取评论第一页获得总评论数
        page_1_resp = requests.get(url=comm_url.format(id, 0), headers=headers)
        page_1_text = json.loads(page_1_resp.text)
        total_comms = int(page_1_text['productCommentSummary']['commentCount'])
        page_nums = total_comms // 10 + 1
        if page_nums > 100:
            page_nums = 100
            # 商品评论链接
            # 改变最后一个数字换页，每页10条评论，最多100页
        for i in range(page_nums):
            self.urlQueue.put(comm_url.format(id, i))

    def get_comments(self):
        robot_comms = ['此用户未填写评价内容',
                       '此用户未及时填写评价内容，系统默认好评！', '']
        while True:
            try:
                # 不阻塞的读取队列数据
                url = self.urlQueue.get_nowait()
            except Exception as e:
                print(e)
                break
            try:
                if self.use_proxy:  # 爬取页数少时不使用代理
                    lock.acquire()
                    proxy_dict = getip.get_random_valid_ip()
                    lock.release()
                    if 'https' in url:
                        # http对应的ip只能请求http的url，同理https
                        proxy_dict = {'https': proxy_dict['http'].replace('http', 'https')}
                    response = requests.get(
                        url, headers=headers, proxies=proxy_dict, timeout=8)
                else:
                    response = requests.get(url, headers=headers, timeout=8)
                text = json.loads(response.text)
                comments = text['comments']
                for comment in comments:
                    id = comment['id']
                    create_date = comment['creationTime']
                    if create_date not in self.pub_time:  # 过滤掉重复发表的评论
                        self.pub_time.add(create_date)
                        content = comment['content'].replace('hellip', '')
                        if content not in robot_comms:
                            self.comm[id] = {}
                            self.comm[id]['create_date'] = create_date
                            self.comm[id]['content'] = content
                            self.comm[id]['scores'] = round(
                                (SnowNLP(content).sentiments() * 100), 2)
                            images = comment.get('images', [])
                            small_images, big_images = [], []
                            if images:
                                small_images = ['https:' + i['imgUrl'] for i in images]
                                big_images = [url.replace('n0/s128x96','shaidan/s616x405') for url in small_images]
                            self.comm[id]['small_images'] = small_images
                            self.comm[id]['big_images'] = big_images
                lock.acquire()
                count = len(self.comm)
                comm_count[self.timeId] = count
                lock.release()
                print('Current Thread Name %s, Url: %s ' % (currentThread().name, url))
            except Exception as e:
                print(e)
                self.urlQueue.put(url)


class TMcomments(CommentParse):
    """获取天猫商品的评论"""
    # 要考虑天猫，天猫超市，天猫国际
    def __init__(self, product_url, timeId):
        super().__init__()
        self.product_url = product_url
        self.pub_time = set()
        self.timeId = timeId
        self.count = 0

    def build_queue(self):
        prod_resp = requests.get(self.product_url, headers=headers)
        match = re.match(
            r'.*?g_config.*?itemId:\"(\d+)\",sellerId:\"(\d+)\".*',
            prod_resp.text,
            re.S)
        if not match:
            return
        itemId = match.group(1)
        sellerId = match.group(2)
        comm_url = 'https://rate.tmall.com/list_detail_rate.htm?itemId={0}&sellerId={1}&currentPage={2}'
        while True:
            page_1_resp = requests.get(comm_url.format(itemId, sellerId, 1), headers=headers)
            if 'rateDetail' in page_1_resp.text:
                break
        try:
            info = json.loads('{' + page_1_resp.text.strip() + '}')
        except BaseException:
            text = re.match('.*?\(({.*})\).*',page_1_resp.text.strip()).group(1)
            info = json.loads(text)
        total_page = int(info['rateDetail']['paginator']['lastPage'])
        for i in range(1, total_page + 1):
            self.urlQueue.put(comm_url.format(itemId, sellerId, i))

    def get_comments(self):
        robot_comms = ['此用户没有填写评论!', '']
        while True:
            try:
                # 不阻塞的读取队列数据
                url = self.urlQueue.get_nowait()
            except Exception as e:
                print(e)
                break
            try:
                if self.use_proxy:  # 爬取页数少时不使用代理
                    lock.acquire()
                    proxy_dict = getip.get_random_valid_ip()
                    lock.release()
                    if 'https' in url:
                        proxy_dict = {'https': proxy_dict['http'].replace('http', 'https')}
                    response = requests.get(url, headers=headers, proxies=proxy_dict, timeout=8)
                else:
                    response = requests.get(url, headers=headers, timeout=8)
                try:
                    text = json.loads('{' + response.text.strip() + '}')
                except BaseException:
                    text = re.match('.*?\(({.*})\).*', response.text.strip()).group(1)
                    text = json.loads(text)
                comments = text['rateDetail']['rateList']
                for comment in comments:
                    id = comment['id']
                    create_date = comment['rateDate']
                    if create_date not in self.pub_time:
                        self.pub_time.add(create_date)
                        content = comment['rateContent'].replace('hellip', '')
                        if content not in robot_comms:
                            self.comm[id] = {}
                            self.comm[id]['create_date'] = create_date
                            self.comm[id]['content'] = content
                            images = comment.get('pics', [])
                            small_images, big_images = [], []
                            if images:
                                small_images = ['https:' + i + '_40x40.jpg' for i in images]
                                big_images = ['https:' + i + '_400x400.jpg' for i in images]
                            self.comm[id]['small_images'] = small_images
                            self.comm[id]['big_images'] = big_images
                            # 获取追加评论
                            append_comm = comment.get('appendComment', '')
                            if append_comm:
                                self.comm[id]['content'] += append_comm['content']
                                self.comm[id]['small_images'] += ['https:' + i + '_40x40.jpg' for i in append_comm['pics']]
                                self.comm[id]['big_images'] += ['https:' + i + '_400x400.jpg' for i in append_comm['pics']]
                            self.comm[id]['scores'] = round((SnowNLP(self.comm[id]['content']).sentiments() * 100), 2)
                lock.acquire()
                count = len(self.comm)
                comm_count[self.timeId] = count
                lock.release()
                print('Current Thread Name %s, Url: %s ' %
                      (currentThread().name, url[-10:]))
            except Exception as e:
                print(e)
                self.urlQueue.put(url)


class AMScomments(CommentParse):
    """获取亚马逊商品评论"""

    def __init__(self, product_url, timeId):
        super().__init__()
        self.product_url = product_url
        self.timeId = timeId
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'}

    def build_queue(self):
        try:
            prod_resp = requests.get(self.product_url, headers=self.headers)
            if 200 <= prod_resp.status_code <= 300:
                selector = Selector(prod_resp)
                total_comms = int(selector.css('.a-size-medium.totalReviewCount::text').extract()[0].replace(',', ''))
            else:
                print('请求商品链接失败：{}'.format(prod_resp.status_code))
                return
        except BaseException:
            return
        total_pages = (total_comms // 10) + 1  # 每次请求20条好了
        comm_url = 'https://www.amazon.cn/hz/reviews-render/ajax/reviews/get/ref=cm_cr_othr_d_paging_btm_{0}'
        for i in range(1, total_pages + 1):
            self.urlQueue.put(comm_url.format(i))

    def get_comments(self):
        while True:
            try:
                # 不阻塞的读取队列数据
                url = self.urlQueue.get_nowait()
            except Exception as e:
                print(e)
                break
            try:
                id = re.match(
                    r'.*(dp|product)/(.+?)[/?].*',
                    self.product_url).group(2)
                post_data = {
                    'reviewerType': 'all_reviews',
                    'filterByStar': 'all_stars',
                    'pageNumber': url.split('_')[-1],
                    'pageSize': '10',
                    'asin': id
                }
                if self.use_proxy:  # 爬取页数少时不使用代理
                    lock.acquire()
                    proxy_dict = getip.get_random_valid_ip()
                    lock.release()
                    if 'https' in url:
                        # http对应的ip只能请求http的url，同理https
                        proxy_dict = {'https': proxy_dict['http'].replace('http', 'https')}
                    response = requests.post(
                        url=url,
                        data=urlencode(post_data),
                        headers=self.headers,
                        proxies=proxy_dict,
                        timeout=8)
                else:
                    response = requests.post(url=url, data=urlencode(
                        post_data), headers=self.headers, timeout=8)
                comments = response.text.split('&&&')
                for comment in comments[3:-4]:
                    comment = comment.strip().replace('["append","#cm_cr-review_list","', '')[:-2]
                    comment = comment.replace('\\', '')
                    selector = Selector(text=comment)
                    id = selector.css('.a-section.review::attr(id)').extract()[0]
                    self.comm[id] = {}
                    self.comm[id]['create_date'] = selector.css('span.review-date::text').extract()[0][2:]
                    content = '\n'.join(selector.css('.a-size-base.review-text::text').extract())
                    if content == '':
                        self.comm.pop(id)
                        continue
                    self.comm[id]['content'] = content
                    self.comm[id]['scores'] = round(
                        (SnowNLP(content).sentiments() * 100), 2)
                    small_images = selector.css('div.review-image-tile-section img::attr(src)').extract()
                    self.comm[id]['small_images'] = small_images
                    big_images = []
                    if small_images:
                        big_images = [url.replace('jpg', '_SY88.jpg') for url in small_images]
                    self.comm[id]['big_images'] = big_images
                lock.acquire()
                count = len(self.comm)
                comm_count[self.timeId] = count
                lock.release()
                print('Current Thread Name %s, Url: %s ' % (currentThread().name, url[-10:]))
            except Exception as e:
                print(e)
                print(url)
                self.urlQueue.put(url)


class SNcomments(CommentParse):
    """获取苏宁商品的评论"""

    def __init__(self, product_url, timeId):
        super().__init__()
        self.product_url = product_url
        self.timeId = timeId

    def build_queue(self):
        id_1, id_2 = re.findall(r'.*/(\d+)/(\d+)\.html', self.product_url)[0]
        if not id_1 or not id_2:
            return
        id_2 = id_2.rjust(18, '0')  # 左边补0至18位
        first_page = 'https://review.suning.com/cmmdty_review/general-{0}-{1}-1-total.htm'.format(
            id_2, id_1)
        try:
            page_1_resp = requests.get(first_page, headers=headers)
            selector = Selector(page_1_resp)
            total_comms = int(selector.css('li[data-type="total"]::attr(data-num)').extract_first(default=0))
        except Exception as e:
            print(e)
            return
        page_nums = total_comms // 10 + 1
        if page_nums > 50:
            page_nums = 50
        comm_url = 'https://review.suning.com/ajax/review_lists/general-{0}-{1}-total-{2}-default-10-----reviewList.htm?callback=reviewList'
        # 评论最多50页
        for page in range(1, page_nums + 1):
            self.urlQueue.put(comm_url.format(id_2, id_1, page))

    def get_comments(self):
        robot_comms = ['买家未及时做出评价，系统默认好评！', '']
        while True:
            try:
                # 不阻塞的读取队列数据
                url = self.urlQueue.get_nowait()
            except Exception as e:
                print(e)
                break
            try:
                if self.use_proxy:  # 爬取页数少时不使用代理
                    lock.acquire()
                    proxy_dict = getip.get_random_valid_ip()
                    lock.release()
                    if 'https' in url:
                        # http对应的ip只能请求http的url，同理https
                        proxy_dict = {'https': proxy_dict['http'].replace('http', 'https')}
                    response = requests.get(url, headers=headers, proxies=proxy_dict, timeout=8)
                else:
                    response = requests.get(url, headers=headers, timeout=8)
                text = re.match(r'reviewList\((.*)\)', response.text).group(1)
                data = json.loads(text)
                comments = data['commodityReviews']
                for comment in comments:
                    id = comment['commodityReviewId']
                    content = comment['content']
                    if content not in robot_comms:
                        self.comm[id] = {}
                        self.comm[id]['content'] = content
                        self.comm[id]['scores'] = round(
                            (SnowNLP(content).sentiments() * 100), 2)
                        self.comm[id]['create_date'] = comment['publishTime']
                        images_info = comment.get('picVideInfo', {})
                        if images_info:
                            small_images = ['https:' + i['url'] + '_100x100.jpg' for i in images_info['imageInfo']]
                            big_images = ['https:' + i['url'] + '_400x400.jpg' for i in images_info['imageInfo']]
                        else:
                            small_images = []
                            big_images = []
                        self.comm[id]['small_images'] = small_images
                        self.comm[id]['big_images'] = big_images
                lock.acquire()
                count = len(self.comm)
                comm_count[self.timeId] = count
                lock.release()
                print('Current Thread Name %s, Url: %s ' % (currentThread().name, url))
            except Exception as e:
                print(e)
                self.urlQueue.put(url)


class DDcomments(CommentParse):
    """获取当当商品的评论"""
    # 当当有防爬，要替换ip

    def __init__(self, product_url, timeId):
        super().__init__()
        self.product_url = product_url
        self.timeId = timeId
        self.pageIndex = 10000

    def build_queue(self):
        # 获取参数categoryPath
        try:
            prod_resp = requests.get(url=self.product_url, headers=headers)
            categoryPath = re.match(
                r'.*?categoryPath":"(.*?)".*',
                prod_resp.text,
                re.S).group(1)
            id = re.match(r'.*?(\d+).*', self.product_url).group(1)
        except Exception as e:
            print(e)
            return
        comm_url = 'http://product.dangdang.com/index.php?r=comment%2Flist&productId={0}&categoryPath={1}&mainProductId={0}&pageIndex={2}'
        try:
            page_1_resp = requests.get(
                url=comm_url.format(
                    id, categoryPath, 1), headers=headers)
            page_1_text = json.loads(page_1_resp.text)
            page_nums = int(page_1_text['data']['list']['summary']['pageCount'])
        except Exception as e:
            print(e)
            return
        for page in range(1, page_nums + 1):
            self.urlQueue.put(comm_url.format(id, categoryPath, page))

    def get_comments(self):
        while True:
            try:
                # 不阻塞的读取队列数据
                url = self.urlQueue.get_nowait()
                if int(url.split('=')[-1]) > self.pageIndex:
                    continue
            except Exception as e:
                break
            try:
                if self.use_proxy:  # 爬取页数少时不使用代理
                    lock.acquire()
                    proxy_dict = getip.get_random_valid_ip()
                    lock.release()
                    if 'https' in url:
                        # http对应的ip只能请求http的url，同理https
                        proxy_dict = {'https': proxy_dict['http'].replace('http', 'https')}
                    response = requests.get(
                        url, headers=headers, proxies=proxy_dict, timeout=8)
                else:
                    response = requests.get(
                        url, headers=headers, timeout=8)  # 要设timeout,否则最后join时会阻塞
                text = json.loads(response.text)['data']['list']['html']
                selector = Selector(text=text)
                comments = selector.css('.comment_items.clearfix')
                if not comments.extract():
                    self.pageIndex = int(url.split('=')[-1])
                for comment in comments:
                    id = comment.css('div.support::attr(data-comment-id)').extract()[0]
                    self.comm[id] = {}
                    self.comm[id]['create_date'] = comment.css('.starline.clearfix span::text').extract()[0]
                    content_list = comment.css('.describe_detail a::text, .describe_detail span::text').extract()
                    self.comm[id]['content'] = ''.join(
                        [content for content in content_list if content not in ['初评', '追评']])
                    if self.comm[id]['content'] == '':
                        self.comm.pop(id)
                        continue
                    self.comm[id]['scores'] = round((SnowNLP(self.comm[id]['content']).sentiments() * 100), 2)
                    self.comm[id]['small_images'] = selector.css('.pic_show.clearfix img::attr(src)').extract()
                    self.comm[id]['big_images'] = selector.css('.pic_show.clearfix img::attr(data-big-pic)').extract()
                lock.acquire()
                count = len(self.comm)
                comm_count[self.timeId] = count
                lock.release()
                print('Current Thread Name %s, Url: %s ' % (currentThread().name, url))
            except Exception as e:
                print(e)
                self.urlQueue.put(url)


def main(url, timeId):
    import time
    start_time = time.time()
    comm_count[timeId] = 0
    host = urlparse(url).netloc
    name = host.split('.')[-2]
    name_dict = {'jd': JDcomments, 'tmall': TMcomments,
                 'amazon': AMScomments, 'suning': SNcomments,
                 'dangdang': DDcomments}
    web_name = name_dict[name]
    web_comment = web_name(url, timeId)
    web_comment.get_all_comments()
    spider_end = time.time()
    print("爬取结束，开始分析，爬取耗时：%s" % (spider_end - start_time))
    comm_count[timeId] = 10000
    mean_score, min_score, max_score, mean_comm, min_comm, max_comm = web_comment.comments_scores()
    hist_imgs = web_comment.hist_scores()
    good_comments, mid_comments, bad_comments = web_comment.show_part_scores()
    analyse_end = time.time()
    print("分析结束，开始生成词云，分析耗时：%s" % (analyse_end - spider_end))
    good_wc = web_comment.build_wordcloud(
        good_comments, os.path.join(
            os.path.dirname(__file__), 'good.jpg'))
    mid_wc = web_comment.build_wordcloud(
        mid_comments, os.path.join(
            os.path.dirname(__file__), 'mid.jpg'))
    bad_wc = web_comment.build_wordcloud(
        bad_comments, os.path.join(
            os.path.dirname(__file__), 'bad.jpg'))
    wc_end = time.time()
    print("生成词云结束，耗时：%s" % (wc_end - analyse_end))
    data = {'mean_score': mean_score, 'mean_comm': mean_comm,
            'min_score': min_score, 'min_comm': min_comm,
            'max_score': max_score, 'max_comm': max_comm,
            'hist_imgs': hist_imgs,
            'good_comments': good_comments,
            'mid_comments': mid_comments,
            'bad_comments': bad_comments,
            'good_wc': good_wc,
            'mid_wc': mid_wc,
            'bad_wc': bad_wc,
            }

    print('总耗时：{} s'.format(time.time() - start_time))
    comm_count.pop(timeId)
    return (json.dumps(data))
