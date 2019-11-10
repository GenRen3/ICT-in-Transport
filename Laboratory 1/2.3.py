###############################################################################
# -------------------STEP 2.3-- DERIVE FILTERING CRITERIA----------------------
###############################################################################
import pymongo as pm
from pprint import pprint
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime


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
# filter: 10 october 2017
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
            "$cond": {"if":{"$ne":["$final_address", "$initial_address"]}, "then":1, "else":0}
        },
        "too_long_time":{ # time too long indicates that the car has not be booked by a user --> TH (thresHold):2 hours
            "$cond": {"if": {"$gt":["duration", THlongBook]}, "then":1, "else":0}
        },
        "too_short_time":{ # time too short indicates that the car never moved -->  TH: 5min
            "$cond": {"if": {"$lt": ["duration", THshortBook]}, "then": 1, "else": 0}

        },
    }},
    {"$match":{
        "$and":[{"moved":1},{"too_long_time":0},{"too_short_time":0}] # returns the cars that actually moved, the parking was not either too short and too long

    }
    }
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

        "duration":{
            "$subtract": ["$final_time", "$init_time"] # returns the duration of rental
        },
        "init_time":1,
        "moved": {  # outlier: did the car actually moved? (change positon)
            "$cond": {"if":{"$ne":["$final_address", "$initial_address"]}, "then":1, "else":0}
        },
        "too_long_time":{ # time too long indicates that the car has problems (visibility or others) TH (thresHold):7 days
            "$cond": {"if": {"$gt":["duration", THlongPark]}, "then":1, "else":0}
        },
        "too_short_time":{ # time too short indicates ...  1min
            "$cond": {"if": {"$lt":["duration", THshortPark]}, "then":1, "else":0}
        },
    }},
    {"$match":{
        "$and":[{"moved":1},{"too_long_time":0},{"too_short_time":0}] # returns the cars that actually moved, the parking was not either too short and too long
    }
    }
]):
    parks+=1
    find_hour(item["init_time"]+time_corrector, parking_list)

# counting element in active booking
countB=0
activeB=[0]*24
for item in activeBook.aggregate([
    {"$match": # stage 1 of the pipeline
         {"$and": [{"city":city},  {"init_time": {"$gte":unixtime_init+int(time_corrector)}}, {"init_time": {"$lt":unixtime_fin+int(time_corrector)}}]}
    },
    {"$project":{
        "_id":0,
        # "hour":{
        #     "$divide": [{"$subtract":["$init_time", unixtime_init]}, hour] # returns the hour in the day (1,..,24)
        # },
        "init_time":1
    }
    }
]):
    countB += 1
    find_hour(item["init_time"]+time_corrector, activeB)

# counting element in active parking
countP=0
activeP=[0]*24
for item in activePark.aggregate([
    {"$match": # stage 1 of the pipeline
         {"$and": [{"city":city},  {"init_time": {"$gte":unixtime_init+int(time_corrector)}}, {"init_time": {"$lt":unixtime_fin+int(time_corrector)}}]}
    },
    {"$project":{
        "_id":0,
        # "hour":{
        #     "$divide": [{"$subtract":["$init_time", unixtime_init]}, hour] # returns the hour in the day (1,..,24)
        # },
        "init_time":1,
    }
    }
]):
    countP += 1
    find_hour(item["init_time"]+time_corrector, activeP)


totale =[]
for elementP, elementB, acP, acB in zip(parking_list, booking_list, activeP, activeB):
    tot = elementB+elementP+acB+acP
    totale.append(tot)

totale1=[]
for elementP, elementB in zip(parking_list, booking_list):
    tot = elementB+elementP
    totale1.append(tot)
print(totale)

parking_list_perc = []
i = 0
for element in parking_list:
    x = element/totale[i] * 100
    parking_list_perc.append(x)
    i+=1


booking_list_perc = []
i = 0
for element in booking_list:
    x = element / totale[i] * 100
    booking_list_perc.append(x)
    i+=1

z = min(min(parking_list_perc), min(booking_list_perc))
s = max(max(parking_list_perc), max(booking_list_perc))

fig = plt.figure()
plt.plot(range(24), booking_list_perc, label="Booking during the day")
plt.plot(range(24), parking_list_perc, label="Parking during the day")
#plt.plot(range(24), totale, label="tot", color="red")
plt.legend()
plt.title("Filtered Rentals in a day in "+city+" October 2017")
plt.grid(True, which='both')
plt.xlabel("hour in the day")
plt.xticks(np.arange(24))
plt.ylabel("% of cars")

# this is to write values in the vertex of the plot
# i = 0
# for elementB, elementP in zip(booking_list_perc, parking_list_perc):
#     plt.annotate(round(elementB, 1), (i, booking_list_perc[i]))
#     plt.annotate(round(elementP,1), (i, parking_list_perc[i]))
#     i+=1

plt.show()

city= city.replace(" ", "")
fig.savefig('/Users/Ciaramella/ICT-Transport-Laboratories/ICT-in-Transport/hourDay'+city+'FILTERED.png')



