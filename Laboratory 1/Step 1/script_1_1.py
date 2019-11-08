###############################################################################
# -------------------------------STEP 1----------------------------------------
###############################################################################

import pymongo as pm
import pprint as pp
import numpy as np
import matplotlib.pyplot as plt

#Here we set the client
client = pm.MongoClient('bigdatadb.polito.it', ssl=True,
         authSource = 'carsharing', tlsAllowInvalidCertificates=True)

#Here we access to a specific collection of the client
db = client['carsharing']
db.authenticate('ictts', 'Ictts16!', mechanism='SCRAM-SHA-1') #authentication

#Collection for Car2go
Car2goPermanentBook = db['PermanentBookings']
Car2goPermanentPark = db['PermanentParkings']
Car2goActiveBook = db['ActiveBookings']
Car2goActivePark = db['ActiveParkings']

#Collection for enjoy
enjoyPermanentBook = db['enjoy_PermanentBookings']
enjoyPermanentPark = db['enjoy_PermanentParkings']
enjoyActiveBook = db['enjoy_ActiveBookings']
enjoyActivePark = db['enjoy_ActiveBookings']


ict_PermanentBook = db['ictts_PermanentBookings']
ict_enjoy_PermanentBook = db['ictts_enjoy_PermanentBookings']

query = {"city":"Torino", "init_fuel": {"$gt":0}} # query about cars in torino with init fuel greater that 0
result = Car2goPermanentBook.find(query).count(True) # searching for vehicles in Turin with fuel greater than 14

## ---------How many documents are present in each collection?-----------------------------------------------1
r = Car2goPermanentBook.count()
print(r)

r = Car2goActiveBook.count()
print(r)

r = Car2goPermanentPark.count()
print(r)

r = Car2goActivePark.count()
print(r)

r = enjoyActivePark.count()
print(r)

r = enjoyActiveBook.count()
print(r)

r = enjoyPermanentPark.count()
print(r)

r = enjoyPermanentBook.count()
print(r)


# -----------For which cities the system is collecting data--------------------------------------------------3
car2goCities = Car2goPermanentBook.distinct("city")
enjoyCities = enjoyPermanentBook.distinct("city")
print(car2goCities)
print(enjoyCities)

# --------When the collection started? ended?----------------------------------------------------------------4
# started:
cursor_start = Car2goPermanentBook.find({}, {"init_time":1, "init_date":1, "_id":0}).sort("init_time", pm.ASCENDING)
pprint(cursor_start[0])
result: "init_time" : 1481650703, ISODate("2016-12-13T18:38:23.000Z") 65 macchine

# ended:
cursor_end= Car2goPermanentBookpermanentBook.find({}, {"init_time":1, "init_date":1, "_id":0}).sort("init_time",pm.DESCENDING)
pprint(cursor_end[0])
result: "init_time" : 1517404293, ISODate("2018-01-31T08:11:33.000Z")


# ---------------What about the timezone of the timestamps?--------------------------------------------------5
# the timezone is GTM (Greenwitch)


# --------How many cars are available in each city?----------------------------------------------------------6
# quale database?
resultFr = Car2goPermanentBook.find({"city":"Frankfurt"}).count()
resultMil = Car2goPermanentBook.find({"city":"Milano"}).count()
resultNy = Car2goPermanentBook.find({"city": "New York City"}).count()
print("Frankfurt, Milano, New York City: ", resultFr, resultMil, resultNy)


# ------How many bookings recorded on the December 2017 in each city?----------------------------------------7
result: Milano: 213364; Frankfurt:53944; NYC:74325 for newyork 5 ore indietro! calcolo del timestamp
locale per 1/12/17 e 31/12/17 e poi per new york ricalcolo tenendo conto del fuso orario di 5 ore indietro
query = {"city":"New York City", "init_time":{
            "$gte": 1512068399, # 1/12/17 at 00:00:00; per new York usa --> 1512068399
            "$lte": 1514743199 # 31/12/2017 at 23:59:59; per New York usa --> 1514743199
            }
        }
result = Car2goPermanentBook.find(query).count()
print(result)


# --------How many bookings have also the alternative transportation modes recorded in each city? -----------8
database key: "public_transport" , "walking"
cursors = Car2goPermanentBook.find({
    "$and": [
        {"city":"Milano"},{"$or":[{"walking.duration": {"$ne":-1}}, {
        "public_transport.duration": {"$ne":-1}
                }
            ]
        }
    ]}).count()

second parameter in find(): {"walking":1, "_id":0, "public_transport":1},
useful if you want to check duration of walking and public transpor

print(cursors)
results: Milano:728653, Frankfurt: 0, New York City: 0

# --------How many bookings have also the alternative transportation modes recorded in each city? -----------8
database key: "public_transport" , "walking"
cursors = Car2goPermanentBook.find({
    "$and": [
        {"city":"Milano"},
        {"$or":[
            {"walking.duration": {"$ne":-1}}, {"public_transport.duration": {"$ne":-1}}
        ]
        }
    ]}).count()

print(cursors)
results: Milano:728653, Frankfurt: 0, New York City: 0
