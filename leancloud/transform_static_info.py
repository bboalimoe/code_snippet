__author__ = 'zhanghengyang'


import leancloud
appid = "pin72fr1iaxb7sus6newp250a4pl2n5i36032ubrck4bej81"
appkey = "qs4o5iiywp86eznvok4tmhul360jczk7y67qj0ywbcq35iia"

leancloud.init(appid, appkey)

from leancloud import Object, Query

UserInfoLog = Object.extend("UserInfoLog")
query = Query(UserInfoLog)
#query.equal_to("objectId", "561cbef900b0712972146438")
logs = query.find()

print type(logs[0])

for log in logs:

    staticInfo = log.get("staticInfo")
    print {"before":staticInfo, "id":log.get("objectId")}
    for key in staticInfo.keys():
        if "-" in key:
            value = staticInfo[key]
            del staticInfo[key]
            index = key.index("-")
            new_key = key[:index]
            new_value = {key[index+1:]:value}
            if new_key in staticInfo.keys():
                staticInfo[new_key].update(new_value)
            else:
                staticInfo[new_key] = new_value
    print {"after":staticInfo, "id":log.get("objectId")}

    log.set("staticInfo", staticInfo)
    log.save()


