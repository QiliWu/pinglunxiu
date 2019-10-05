# pinglunxiu



在线抓取和分析天猫，京东，苏宁，当当和亚马逊电商平台商品评论，并对评论内容的进行简单的情感分析和词云分析

1. 多线程爬虫的实现

根据用户输入的商品首页链接，分析生成商品评论的url，将所有的评论url放入实例变量队列中（Queue）。

通过threading开启多线程，每个线程都从队列中获取评论url, 对其发出请求，并从返回的结果中提取出每条评论的有效信息（评论内容，图片，发表时间等），存放到实例变量self.comm字典中。

2. 避免爬虫被禁的策略

使用代理ip，每个请求都随机更换ip。

设置headers信息

3. 情感分析

首先利用实现整理好的好评（pos.txt）和差评（neg.txt）集对snownlp中情感打分模型进行训练

再使用训练好的snownlp对self.comm中的每条评论进行打分

对所有评论的分数生成分布直方图

将评论按分数分为好评（70-100），中评（40-70）， 差评（0-40）三个区段。使用wordcloud对每个区段的评论内容生成词云图。

4. 需要使用到的库

requests, threading, snownlp, wordcloud, jieba, matplotlib, pillow, numpy, scrapy(使用了其中的选择器）




![pinglunxiu](https://github.com/QiliWu/pinglunxiu/blob/master/pinglunxiu.png)
