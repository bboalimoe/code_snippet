# -*- coding: utf-8 -*-
__author__ = 'zhanghengyang'

from pymongo import MongoClient
from coordinate_trans import wrong_baidu_to_right_baidu
import arrow
import json
import requests
import leancloud
from leancloud import Query, Object

# mongodb init
client = MongoClient("mongodb://senzhub:Senz2everyone@119.254.111.40:27017")
db = client.RefinedLog

# senz.log.tracer appid,appkey
tracer_app_id = "9ra69chz8rbbl77mlplnl4l2pxyaclm612khhytztl8b1f9o"
tracer_app_key = "1zohz2ihxp9dhqamhfpeaer8nh1ewqd9uephe9ztvkka544b"

#找出所有ios用户
leancloud.init(tracer_app_id, tracer_app_key)
User = Object.extend("_User")
user_query = Query(User)
user_query.equal_to("os","ios")
userids = [ user.get("objectId")for user in user_query.find()]

#从哪个时间点之前开始更新数据
axis_wrong_date = '2015-11-14T12:34:00.000+08:00'
time = arrow.get(axis_wrong_date)


cur_ts = arrow.now().timestamp * 1000

for location in db.UserLocation.find({"timestamp": {"$lt":cur_ts}}):

    try:
        print location.get("objectId")
        lat = location.get("location")["lat"]
        lng = location.get("location")["lng"]
        timestamp = location.get("timestamp")
        if timestamp < time.timestamp * 1000 and user_id in userids :
            right_location = wrong_baidu_to_right_baidu({"lat":lat, "lng":lng})
            lat = right_location["lat"]
            lng = right_location["lng"]
        user_id = location.get("user_id")
        id = location.get("_id")

        r_p = {
            "user_trace":[
                        {
                        "timestamp":timestamp,
                        "location":{
                        "latitude":lat,
                        "__type":"GeoPoint",
                        "longitude":lng
                        }
                    }
                    ],
                    "dev_key":"senz",
                    "userId": user_id
                    }

        poi_url = "https://api.trysenz.com" +  "/pois/location_probability/"
        headers = {"Content-Type":"application/json"}
        data = json.dumps(r_p)
        res = requests.post(poi_url, data=data, headers=headers)
        content = json.loads(res.content)
        near_home_office = content["home_office_label"]
        poiProbLv2 = poiProbLv1 = {}
        for key in content["results"]["poi_probability"][0].keys():
            poiProbLv1.setdefault(key, content["results"]["poi_probability"][0][key]["level1_prob"])
            for lv2_key in content["results"]["poi_probability"][0][key]["level2_prob"].keys():
                poiProbLv2.setdefault(lv2_key, content["results"]["poi_probability"][0][key]["level2_prob"][lv2_key])

        result = db.UserLocation.update_one(
            {"_id":id},
            {
                "$set":{
                    "near_home_office":near_home_office,
                    "poiProbLv2":poiProbLv2,
                    "poiProbLv1":poiProbLv1,
                    "updateAt": arrow.now().ctime(),
                    "location": {"lat":lat,"lng":lng}

                },
                "$currentDate": {"lastModified": True}
            }
        )
        print result.matched_count,"objects updated"
    except Exception, e:
        print Exception,e



'''
  poi_request = {'userId': user_id, 'dev_key': dev_key, 'locations': user_trace}
    headers = {'X-senz-Auth': POI_AUTH_KEY}
    poi_greenlet = gevent.spawn(requests.post, POI_URL, data=json.dumps(poi_request), headers=headers)
'''

demo_rqs_params = {
"user_trace":[
{
"timestamp":1447745708586,
"location":{
"latitude":39.9804621,
"__type":"GeoPoint",
"longitude":116.3091294
}
}
],
"dev_key":"senz",
"userId":"55f788f4ddb25bb7713125ef"
}