import pymongo as pm


#Here we set the client
client = pm.MongoClient('bigdatadb.polito.it', ssl=True,
         authSource = 'carsharing', tlsAllowInvalidCertificates=True)

#Here we access to a specific collection of the client
db = client['carsharing']
db.authenticate('ictts', 'Ictts16!', mechanism='SCRAM-SHA-1') #authentication

#Collection for Car2go
permanentBook = db['PermanentBookings']
permanentPark = db['PermanentParkings']
activeBook = db['ActiveBookings']
activePark = db['ActiveParkings']

#Collection for enjoy
enjoyPermanentBook = db['enjoy_PermanentBookings']
enjoyPermanentPark = db['enjoy_PermanentParkings']
enjoyActiveBook = db['enjoy_ActiveBookings']
enjoyActivePark = db['enjoy_ActiveBookings']


ict_PermanentBook = db['ictts_PermanentBookings']
ict_enjoy_PermanentBook = db['ictts_enjoy_PermanentBookings']

step_time = {"city":["Milano", "Frankfurt", "New York City"], "time": [-3600,-3600,18000]} # dictionary for time step

# ----------------------------------------------------------------------------------------------------------------------
# -------------------------How many documents are present in each collection?------------------------------------------1
# ----------------------------------------------------------------------------------------------------------------------

# r = permanentBook.count()
# print(r)
#
# r = activeBook.count()
# print(r)
#
# r = permanentPark.count()
# print(r)
#
# r = activePark.count()
# print(r)
#
# r = enjoyActivePark.count()
# print(r)
#
# r = enjoyActiveBook.count()
# print(r)
#
# r = enjoyPermanentPark.count()
# print(r)
#
# r = enjoyPermanentBook.count()
# print(r)

# ----------------------------------------------------------------------------------------------------------------------
# ---------------------For which cities the system is collecting data--------------------------------------------------3
# ----------------------------------------------------------------------------------------------------------------------
# cities = permanentBook.distinct("city")
# enjoyCities = enjoyPermanentBook.distinct("city")
# print(cities)
# print(enjoyCities)

# ----------------------------------------------------------------------------------------------------------------------
# --------------------------------When the collection started? ended?--------------------------------------------------4
# ----------------------------------------------------------------------------------------------------------------------
# started:
# cursor_start = permanentBook.find({}, {"init_time":1, "init_date":1, "_id":0}).sort("init_time", pm.ASCENDING)
# pprint(cursor_start[0])
# # result: "init_time" : 1481650703, ISODate("2016-12-13T18:38:23.000Z") 65 macchine
#
# # ended:
# cursor_end= permanentBookpermanentBook.find({}, {"init_time":1, "init_date":1, "_id":0}).sort("init_time",pm.DESCENDING)
# pprint(cursor_end[0])
# result: "init_time" : 1517404293, ISODate("2018-01-31T08:11:33.000Z")

# ----------------------------------------------------------------------------------------------------------------------
# -------------------------What about the timezone of the timestamps?--------------------------------------------------5
# ----------------------------------------------------------------------------------------------------------------------
# the timezone is GTM (Greenwitch)

# ----------------------------------------------------------------------------------------------------------------------
# ------------------How many cars are available in each city?----------------------------------------------------------6
# ----------------------------------------------------------------------------------------------------------------------
# resultFr = permanentBook.find({"city":"Frankfurt"}).count()
# resultMil = permanentBook.find({"city":"Milano"}).count()
# resultNy = permanentBook.find({"city": "New York City"}).count()
# print("Frankfurt, Milano, New York City: ", resultFr, resultMil, resultNy)
# step_time = {"city":["Milano", "Frankfurt", "New York City"], "time": [-3600,-3600,18000]}

# result = []
# for city in step_time["city"]:
#     print(city)
#     a = permanentBook.distinct('plate', {"city":city})
#     print(len(a))
#     result.append(len(a))
#
# print(result)


# ----------------------------------------------------------------------------------------------------------------------
# ----------------How many bookings recorded on the December 2017 in each city?----------------------------------------7
# ----------------------------------------------------------------------------------------------------------------------
# result: Milano: 209774; Frankfurt:52359; NYC:71954
# import time
# import datetime
# ini = datetime.date(2017,12,1)
# fin = datetime.date(2017,12,31)
# step_time = {"city":["Milano", "Frankfurt", "New York City"], "time": [-3600,-3600,18000]} # dictionary for time step
#
# init_time = time.mktime(ini.timetuple())
# fin_time = time.mktime(fin.timetuple())
#
# result = []
# for city in step_time["city"]:
#     corrector = int(step_time["time"][step_time["city"].index(city)]) # correcting time to zone
#     a = permanentBook.find({"city":city, "init_time":{"$gte": init_time+corrector, "$lte":fin_time+corrector}}).count()
#     result.append(a)
# print(result)

# ----------------------------------------------------------------------------------------------------------------------
# --------How many bookings have also the alternative transportation modes recorded in each city? ---------------------8
# ----------------------------------------------------------------------------------------------------------------------
# results: Milano:728653, Frankfurt: 0, New York City: 0
# database key: "public_transport" , "walking"
# result = []
# for city in step_time["city"]:
#     print(city)
#     result.append(permanentBook.find({
#     "$and": [
#         {"city":city},{"$or":[{"walking.duration": {"$ne":-1}}, {
#         "public_transport.duration": {"$ne":-1}
#                 }
#             ]
#         }
#     ]}).count())
#
# print(result)
# second parameter in find(): {"walking":1, "_id":0, "public_transport":1},
#useful if you want to check duration of walking and public transpor


