# -*- coding: utf-8 -*-
## 项目描述
#  将mongodb中没有数据，从tracer中取出来。然后走一遍请求流程

from pymongo import MongoClient
from bson.objectid import ObjectId
import pymongo
import leancloud
import uuid
from leancloud import Query, Object
import requests
import datetime,time
import arrow

start = datetime.datetime.strptime('2015-10-30', '%Y-%m-%d')
start = time.mktime(start.timetuple())
end= start + 24 * 60 * 60
uid = '55898213e4b0ef61557555a8'

tracer_app_id = "9ra69chz8rbbl77mlplnl4l2pxyaclm612khhytztl8b1f9o"
tracer_app_key = "1zohz2ihxp9dhqamhfpeaer8nh1ewqd9uephe9ztvkka544b"

timeline_APP_ID = "pin72fr1iaxb7sus6newp250a4pl2n5i36032ubrck4bej81"
timeline_APP_KEY = "qs4o5iiywp86eznvok4tmhul360jczk7y67qj0ywbcq35iia"


UserLocationFields = {
    "street": 1,
    "radius": 1,
    "updatedAt": 1,
    "isTrainingSample": 1,
    "createdAt": 1,
    "userRawdataId": 1,
    "city": 1,
    "user_id": 1,
    "objectId": 1,
    "location": 1,
    "senzedAt": 1,
    "province": 1,
    "timestamp": 1,
    "nation": 1,
    "district": 1,
    "processStatus": 1,
    "street_number": 1
}


leancloud.init(tracer_app_id, tracer_app_key)
# UserMotion = Object.extend("UserMotion")
Log = Object.extend("Log");
log_query = Query(Log);
# motion_query = Query(UserMotion)
client = MongoClient("mongodb://root:Senz2everyone@119.254.111.40:27017")
db = client.senz
#current_timestamp = arrow.now().timestamp * 1000

#log_query.equal_to("type","location")
#log_query.less_than("timestamp",current_timestamp)


#count_query = Query(Log)
#count_query.equal_to("type","location")
#count_query.less_than("timestamp",current_timestamp)

#location_mounts = count_query.count()

#print location_mounts
json_file = "/Users/zhanghengyang/Log日志_2015_11_10/Log.json"
import ijson
log_objects = ijson.items(open("/Users/zhanghengyang/Log日志_2015_11_10/Log.json","rb"),"results.item")

#for times in range(location_mounts/ 500 + 1):
log_list = []
import json
for log in log_objects:
    s = json.dumps(log)
    log = json.loads(s)
    print type(log)
    db.LeancloudLogBackup.insert_one(log)
    # log_list.append(log)
    # if(len(log_list) > 500):
    #     #db.LeancloudLogBackup.insert_many(log_list)
    #     log_list = []

#
# a = 0
# t_list = []
# for i in range(10):
#     t_list.append(i)
#
#     if (len(t_list) > 2):
#         t_list = []
#     print t_list


   # if log["type"] == "sensor" or log["type"] == "accSensor":
   #
   #      if db.UserMotion.find_one({"userRawdataId":log.get("objectId")}) == None:
   #              print log.get("objectId")
   #              print "type is","sensor"
   #              log_query = Query(Log)
   #              leancloud_log = log_query.get(log.get("objectId"))
   #              leancloud_log.set("update","2015-11-10")
   #              leancloud_log.save()
   #
   # if log["type"] == "location" and log["source"] == "baidu offline converter":
   #
   #      if db.UserLocation.find_one({"userRawdataId":log.get("objectId")}) == None:
   #              print log.get("objectId")
   #              print "type is","location"
   #              log_query = Query(Log)
   #              leancloud_log = log_query.get(log.get("objectId"))
   #              leancloud_log.set("update","2015-11-10")
   #              leancloud_log.save()



#
# a = db.UserLocation.find({'timestamp': {'$gt': start, '$lt': end},
#                                 'user_id': uid})

#a = db.UserLocation.find({},UserLocationFields).limit(500).sort([('timestamp', 1)])

#create index
#db.UserLocation.createIndex({"timestamp":1})

#
# skip = 0
# where = {'timestamp': {'$gt': 1445788799000, '$lt': 1446652799000}, 'user_id': u'55d845e100b0d7b2266ac668'}
# for i in range(100):
#
#     a = db.UserLocation.find(where,UserLocationFields).skip(skip).limit(500).sort([('timestamp', 1)])
#     xx = 0
#     for x in a:
#         #print x
#         print xx
#         xx += 1
#     print i
#     skip = i*500



# print a
#
# #print a.
# xx = 0
# for x in a:
#     #print x
#     print xx
#     xx += 1




#print db.UserLocation.find_one({"objectId":"5628e82b60b25974b26c075a"})
# import timeit
# for times in range(count_query.count() / 100 + 1):
#
#     motion_query.limit(100)
#     motion_query.skip(times * 100)
#     objects = motion_query.find()
#
#     print len(objects)
#     print "当前时间", timeit.default_timer()
#     new_objs = []
#     for object in objects:
#         id = object["user"]["objectId"]
#         del object["user"]
#         object["user_id"] = id
#         print object
#         #db.UserMotion.insert_one(object)
#         new_objs.append(object)
#     db.UserMotion.insert_many(new_objs)



print "finished"
# print db.Log.find_one()
# log = db.Log.find_one()
# old_log = {u'url': None, u'timestamp': 1422230000000.0, u'updatedAt': 1440154877264.0, u'value': [], u'source': u'internal', u'location': {u'lat': 16, u'lng': 120}, u'file': None, u'locationRadius': None, u'type': u'accSensor'}
#
# print type(old_log)
#
# print db.Log.count()
#
# # #print db.User.find_one()
# # user_id = str(db.User.find_one()["_id"])
# for object in objects[1:2]:
#     print obje
#print db.UserLocation.find_one()



# for x in range(1000000):
#     uu = uuid.uuid4()
#     # new_log = old_log
#     # print uu
#     # print type(new_log)
#     # print "_id" in new_log.keys()
#     # new_log.setdefault("_id", uu)
#     # print "old log", old_log
#     # print "new_log", new_log
#     result = db.Log.insert_one({u'url': None, u'timestamp': x, u'updatedAt': 1440154877264.0, u'value': [], u'source': u'internal', u'location': {u'lat': 16, u'lng': 120}, u'file': None, u'locationRadius': None, u'type': u'accSensor'}
# )
#     #results =  db.Log.insert_many([old_log for i in range(10)])
#     print result.inserted_id
