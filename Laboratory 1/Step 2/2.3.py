###############################################################################
# -------------------STEP 2.3-- DERIVE FILTERING CRITERIA----------------------
###############################################################################
import pymongo as pm
from pprint import pprint
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import os


# configuration
client = pm.MongoClient('bigdatadb.polito.it', ssl=True, authSource = 'carsharing', tlsAllowInvalidCertificates=True)
db = client['carsharing'] #Choose the database to use
db.authenticate('ictts', 'Ictts16!')# , mechanism='MONGODB-CR')


permanentBook = db['PermanentBookings']
permanentPark = db['PermanentParkings']
activeBook = db['ActiveBookings']
activePark = db['ActiveParkings']

enjoy_permanentBook = db['enjoy_PermanentBookings']
enjoy_permanentPark = db['enjoy_PermanentParkings']
enjoy_activeBook = db['enjoy_ActiveBookings']
enjoy_activePark = db['enjoy_ActiveBookings']

ict_PermanentBook = db['ictts_PermanentBookings']
ict_enjoy_PermanentBook = db['ictts_enjoy_PermanentBookings']


# ------------aggregate rentals per hour of the day; plot # booked/parked cars (or %) per hour versus time of day------2

import time
import datetime
ini = datetime.date(2017,10,1)
fin = datetime.date(2017,10,31)

from datetime import datetime

def find_hour(timeStamp, list):
    day_obj = datetime.fromtimestamp(timeStamp)
    hour = day_obj.hour
    for i in range(24):
        if hour>=i and hour< (i+1):
            list[i] += 1


step_time = {"city":["Milano", "Frankfurt", "New York City"], "time": [-3600,-3600,18000]}

unixtime_init = time.mktime(ini.timetuple())
unixtime_fin = time.mktime(fin.timetuple())

city = "Frankfurt"
time_corrector = step_time["time"][step_time["city"].index(city)] # correcting time to zone

THlongBook = 3*60*60 # no more than 3 hours rental for booking
THshortBook = 5*60 # no less han 5 minutes of rentals for booking
booking_list=[0]*24
parking_list = [0]*24
books = 0

for item in permanentBook.aggregate([
    {"$match": # stage 1 of the pipeline
         {"$and": [{"city":city},  {"init_time": {"$gte":unixtime_init+int(time_corrector)}}, {"init_time": {"$lt":unixtime_fin+int(time_corrector)}}]}
    },
    {"$project":{
        "_id":0,
        "init_time":1,
        "duration":{
            "$subtract": ["$final_time", "$init_time"] # returns the duration of rental
        },
        "moved": {  # outlier: did the car actually moved? (change positon)
            "$strcasecmp": ["$final_address", "$initial_address"] # if not moved, returns 0
        },
    }},
    {"$match":{
        "moved": {"$ne": 0},
        "duration":{
            "$gt": THshortBook,
            "$lt":THlongBook
        }
    }},
]):
    books+=1
    find_hour(item["init_time"]+time_corrector, booking_list)

parks = 0
THlongPark = 7*24*60*60 # no more than 7 days stopped in the park (maybe gps problems? however car has problem)
THshortPark = 1*60 # no less han 1 minutes of rentals for parking
for item in permanentPark.aggregate([
    {"$match": # stage 1 of the pipeline
         {"$and": [{"city":city},  {"init_time": {"$gte":unixtime_init+int(time_corrector)}}, {"init_time": {"$lt":unixtime_fin+int(time_corrector)}}]}
    },
    {"$project":{
        "_id":0,
        "init_time":1,
        "duration":{
            "$subtract": ["$final_time", "$init_time"] # returns the duration of rental
        },
        "moved": {  # outlier: did the car actually moved? (change positon)
            "$strcasecmp": ["$final_address", "$initial_address"] # returns 0 if the car moved
        },
    }},
    {"$match":{
        # "moved": {"$ne": 0},
        "duration":{
            "$gt": THshortPark,
            "$lt":THlongPark
        }
    }},
]):
    parks+=1
    find_hour(item["init_time"]+time_corrector, parking_list)


notCleanBooking_list=[0]*24
notCleanParking_list=[0]*24
for item in permanentBook.aggregate([
    {"$match": # stage 1 of the pipeline
         {"$and": [{"city":city},  {"init_time": {"$gte":unixtime_init+int(time_corrector)}}, {"init_time": {"$lt":unixtime_fin+time_corrector}}]}
    },
    {"$project":{
        "_id":0,
        "init_time":1
    }
    }
]):
    find_hour(item["init_time"]+time_corrector, notCleanBooking_list)

for item in permanentPark.aggregate([
    {"$match": # stage 1 of the pipeline
         {"$and": [{"city":city},  {"init_time": {"$gte":unixtime_init+int(time_corrector)}}, {"init_time": {"$lt":unixtime_fin+int(time_corrector)}}]}
    },
    {"$project":{
        "_id":0,

        "init_time":1
    }
    }
]):
    find_hour(item["init_time"]+time_corrector, notCleanParking_list)


path = "./"
fig = plt.figure()
plt.plot(range(24), booking_list, label="Cleaned books", color="blue", alpha="0.5")
plt.plot(range(24), notCleanBooking_list, label="not cleaned books", color="blue",alpha="0.5",  linestyle="-.")
plt.plot(range(24), parking_list, label="Cleaned parks", color="green", alpha="0.5")
plt.plot(range(24), notCleanParking_list, label="not cleaned parks", color="green", linestyle="-.", alpha="0.5")
plt.title("Rentals in a day in "+city+" October 2017")
plt.grid(True, which='both')
plt.xlabel("Hour in the day [hr]")
plt.xticks(np.arange(24))
plt.ylabel("Total # of cars")
plt.legend()
#plt.show()
city= city.replace(" ", "")
fig.savefig(''+city+'FILTERED.png')

