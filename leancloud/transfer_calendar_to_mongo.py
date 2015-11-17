# -*- coding: utf-8 -*-
from pymongo import MongoClient
from bson.objectid import ObjectId
import pymongo
import leancloud
import uuid
from leancloud import Query, Object
import requests

APP_ID = "pin72fr1iaxb7sus6newp250a4pl2n5i36032ubrck4bej81"
APP_KEY = "qs4o5iiywp86eznvok4tmhul360jczk7y67qj0ywbcq35iia"
leancloud.init(APP_ID, APP_KEY)
UserCalendar = Object.extend("UserCalendar")
motion_query = Query(UserCalendar)

client = MongoClient("mongodb://senzhub:Senz2everyone@119.254.111.40:27017")

count_query = Query(UserCalendar)

db = client.RefinedLog
#print db.UserCalendar.count()
import timeit
for times in range(count_query.count() / 100 + 1):

    motion_query.limit(100)
    motion_query.skip(times * 100)
    objects = motion_query.find()

    print len(objects)
    print "当前时间", timeit.default_timer()
    new_objs = []
    for object in objects:
        id = object["user"]["objectId"]
        del object["user"]
        object["user_id"] = id
        print object
        #db.UserCalendar.insert_one(object)
        new_objs.append(object)
    db.UserCalendar.insert_many(new_objs)



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
