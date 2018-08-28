# -*- coding: utf-8 -*-
# python36
__author__ = 'wuqili'

#自动登录蘑菇ip, 然后获取代理ip的连接
# 1. 验证码识别
# 2.登录蘑菇ip,
# 3. 购买ip
import time
import requests
from io import BytesIO
from PIL import Image
import pytesseract
import json
import re
import sqlite3
from scrapy.selector import Selector


class GetIP():
    """从代理网站爬取ip, 栋爬虫使用"""
    def __init__(self):
        self.session = requests.Session()
        self.conn = sqlite3.connect('ip_pool.sqlite',check_same_thread=False)
        self.cur = self.conn.cursor()
        create_table = "CREATE TABLE  IF NOT EXISTS ippool (proxy text);"
        self.cur.execute(create_table)

    def login(self):
        """蘑菇ip账号自动登录"""
        # 直接将验证码图片破解，获取验证码
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
        captcha_url = 'http://www.moguproxy.com/proxy/validateCode/createCode?time={} '.format(
            int(time.time() * 1000))
        r = self.session.get(captcha_url) #请求验证码图片链接
        im = Image.open(BytesIO(r.content))   #直接读取bytes数据，生成图片对象
        width, height = im.size
        #获取图片中的颜色，返回列表[(counts, color)...]
        color_info = im.getcolors(width*height)
        sort_color = sorted(color_info, key=lambda x: x[0], reverse=True)
        #将背景全部改为白色, 提取出字，每张图片一个字
        char_dict = {}
        for i in range(1, 6):
            start_x = 0
            im2 = Image.new('RGB', im.size, (255, 255, 255))
            for x in range(im.size[0]):
                for y in range(im.size[1]):
                    if im.getpixel((x, y)) == sort_color[i][1]:
                        im2.putpixel((x, y), (0, 0, 0))
                        if not start_x:
                            start_x = x
                    else:
                        im2.putpixel((x, y), (255, 255, 255))
            char = pytesseract.image_to_string(im2, lang='normal',config='--psm 10')
            char_dict[start_x] = char
        code = ''.join([item[1] for item in sorted(char_dict.items(), key=lambda i:i[0])])
        login_url = 'http://www.moguproxy.com/proxy/user/login?mobile=1234567890&password=abcd2018&code={}'.format(code)
        self.session.get(login_url)

    def crawl_api(self):
        """爬取代理ip并存入sqlit3数据库"""
        api_url = 'http://piping.mogumiao.com/proxy/api/get_ip_al?appKey=????????????????&count=5&expiryDate=0&format=1&newLine=2'
        mark = True
        while mark:
            try:
                response = self.session.get(api_url)
                mark = False
            except Exception as e:
                print(e)
        response = self.session.get(api_url)
        while 'port' not in response.text:
            self.login()
            response = self.session.get(api_url)
        data = json.loads(response.text,encoding='utf8')
        for proxy in data['msg']:
            port = proxy['port']
            ip = proxy['ip']
            http_proxy_ip = 'http://' + ip + ':' + port
            self.cur.execute("insert into ippool (proxy) values('{}')".format(http_proxy_ip))
            self.conn.commit()
        print('成功下载新的ip')


    def check_ip(self, proxy_dict):
        """通过请求www.123cha.com来验证ip是否有效"""
        http_url = 'http://www.123cha.com/'
        if not proxy_dict:
            return False
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'}
            response = requests.get(
                http_url, headers=headers, proxies=proxy_dict, timeout=2)
            response.raise_for_status()
            code = response.status_code
            if code >= 200 and code < 300:
                response = Selector(response)
                text = response.css('div.location').extract()[0]
                for key, value in proxy_dict.items():
                    ip = re.match(r'.*://(.*?):\d+', value).group(1)
                    if ip in text:
                        return True
                    else:
                        return False
            else:
                return False
        except BaseException:
            return False


    def get_random_valid_ip(self):
        # 从数据库随机提取ip，验证有效性，直到获取到有效的ip
        self.cur.execute(
            'SELECT proxy FROM ippool ORDER BY random() LIMIT 1')
        data = self.cur.fetchall()
        if data:
            for ip_info in data:
                proxy = ip_info[0]
                proxy_dict = {'http': proxy}
                if self.check_ip(proxy_dict):
                    return proxy_dict
                else:
                    self.cur.execute(
                        'DELETE FROM ippool WHERE proxy= ?', (proxy,))
                    self.conn.commit()
                    return self.get_random_valid_ip()

        else:
            print("数据库中没有ip了，重新下载新的ip")
            self.crawl_api()
            return self.get_random_valid_ip()

    def close_conn(self):
        self.conn.close()

