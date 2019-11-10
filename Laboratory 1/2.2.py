###############################################################################
# -------------------------------STEP 2.2 ----------------------------------------
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

booking_list=[0]*24
parking_list = [0]*24
books = 0
info_date=[]


for item in permanentBook.aggregate([
    {"$match": # stage 1 of the pipeline
         {"$and": [{"city":city},  {"init_time": {"$gte":unixtime_init+int(time_corrector)}}, {"init_time": {"$lt":unixtime_fin+time_corrector}}]}
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
    #book.append(item["hour"])
    books+=1
    find_hour(item["init_time"]+time_corrector, booking_list)

parks = 0
for item in permanentPark.aggregate([
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
    #book.append(item["hour"])
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
        "init_time":1
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

print("considering only permanent collections:     ", totale1)
print("considering also active booking and parking:", totale)

parking_list_perc = []
booking_list_perc = []

for elementP, elementB, i in zip(parking_list, booking_list, range(len(booking_list))):
    p = elementP / totale[i] * 100
    b = elementB / totale[i] * 100
    booking_list_perc.append(b)
    parking_list_perc.append(p)


z = min(min(parking_list_perc), min(booking_list_perc))
s = max(max(parking_list_perc), max(booking_list_perc))

fig = plt.figure()
plt.plot(range(24), booking_list_perc, label="Booking during the day")
plt.plot(range(24), parking_list_perc, label="Parking during the day")
#plt.plot(range(24), totale, label="tot", color="red")
plt.legend()
plt.title("Rentals in a day in "+city+" October 2017")
plt.grid(True, which='both')
plt.xlabel("hour in the day")
plt.xticks(np.arange(24))
plt.yticks(np.arange(z, s , 2.0))
plt.ylabel("% of cars")

# this is to write values in the vertex of the plot
# i = 0
# for elementB, elementP in zip(booking_list_perc, parking_list_perc):
#     plt.annotate(round(elementB, 1), (i, booking_list_perc[i]))
#     plt.annotate(round(elementP,1), (i, parking_list_perc[i]))
#     i+=1

plt.show()

city= city.replace(" ", "")
fig.savefig('/Users/Ciaramella/ICT-Transport-Laboratories/ICT-in-Transport/hourDay'+city+'.png')



