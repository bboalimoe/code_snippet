# -*- coding: utf-8 -*-
__author__ = 'zhanghengyang'

import requests, json

url="http://sendcloud.sohu.com/webapi/mail.send.json"

# 不同于登录SendCloud站点的帐号，您需要登录后台创建发信子帐号，使用子帐号和密码才可以进行邮件的发送。
params = {"api_user": "senz_test_3xTpiB", \
"api_key" : "0KEO6ysamOyGnunf",\
"from" : "service@sendcloud.im", \
"fromname" : "SendCloud测试邮件", \
"to" : "danielzhang-1@163.com", \
"subject" : "来自SendCloud的第一封邮件！", \
"html": "你太棒了！你已成功的从SendCloud发送了一封测试邮件，接下来快登录前台去完善账户信息吧！", \
"resp_email_id": "true"
}

r = requests.post(url, files={}, data=params)
print r.text