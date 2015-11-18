


for location in db.UserLocation.find({"timestamp": {"$lt":cur_ts}}).sort("timestamp", -1):



    print location.get("objectId")
    lat = location.get("location")["lat"]
    lng = location.get("location")["lng"]
    timestamp = location.get("timestamp")
    print timestamp
    user_id = location.get("user_id")
    isIosAxisConverted = location.get("isIosAxisConverted",0)
    updatedTag = location.get("updatedTag", "")

    if time_before < timestamp < time.timestamp * 1000 and user_id in userids and isIosAxisConverted == 0 :
        right_location = wrong_baidu_to_right_baidu({"lat":lat, "lng":lng})
        lat = right_location["lat"]
        lng = right_location["lng"]
        isIosAxisConverted = 1
    id = location.get("_id")

    if updatedTag != defaultTag:

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
        if "home_office_label" not in content.keys():
            near_home_office = "unknown"
        else:
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
                    "updatedAt": arrow.now().ctime(),
                    "location": {"lat":lat,"lng":lng},
                    "isIosAxisConverted":isIosAxisConverted,
                    "updatedTag": defaultTag

                },
                "$currentDate": {"lastModified": True}
            }
        )
        print result.matched_count,"objects updated"
