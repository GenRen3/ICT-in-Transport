###############################################################################
# -------------------------------STEP 2----------------------------------------
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

# -------------Derive the CDF of booking/parking duration, and plot them.----------------------------------1-1c
# filter: october 2017
# for New york: first_oct_day=1506801600, last_oct_day = 1509494400
# for Milano-Frankfurt: first_oct_day: 1506816000 last_oct_day:1509408000
first_oct_day = 1506816000
last_oct_day = 1509408000
city = "Frankfurt"
book_duration = []
book_week_1 = []
book_week_2 = []
book_week_3 = []
book_week_4 = []
book_week_5 = []

for item in Car2goPermanentBook.aggregate([
    {"$match": # stage 1 of the pipeline
         {"$and": [{"city":city},  {"init_time": {"$gte":first_oct_day}}, {"init_time": {"$lte": last_oct_day}}]}
    },
    {"$project":{
        "_id":0,
        "durationBook": {
            "$divide": [{"$subtract":["$final_time", "$init_time"]}, 60]
        },
        "week":{
            "$divide": [{"$subtract":["$init_time", 1506801599]}, 604800] # week returns the number of week (1,2,3,4) in october
        }
    }
    },
]):
    book_duration.append(item["durationBook"])
    if int(item["week"]) == 1:
        book_week_1.append(item["durationBook"])
    elif int(item["week"]) == 2:
        book_week_2.append(item["durationBook"])
    elif int(item["week"]) == 3:
        book_week_3.append(item["durationBook"])
    elif int(item["week"]) == 4:
        book_week_4.append(item["durationBook"])
    else:
        book_week_5.append(item["durationBook"])


book = sorted(book_duration)

park_duration = []
park_week_1 = []
park_week_2 = []
park_week_3 = []
park_week_4 = []
park_week_5 = []

for item in Car2goPermanentPark.aggregate([
    {"$match": # stage 1 of the pipeline
         {"$and": [{"city":city}, {"init_time": {"$gte":first_oct_day}}, {"init_time":{"$lte":last_oct_day}}]}
    },
    {"$project":{ # stage 2
         "_id":0,
         "durationPark":{
             "$divide":[{"$subtract":["$final_time", "$init_time"]}, 60]
         },
         "week":{
            "$divide": [{"$subtract":["$init_time", 1506801599]}, 604800] # week returns the number of week (1,2,3,4) in october
         }
    }
    }
]):
    park_duration.append(item["durationPark"])
    if int(item["week"]) == 1:
        park_week_1.append(item["durationPark"])
    elif int(item["week"]) == 2:
        park_week_2.append(item["durationPark"])
    elif int(item["week"]) == 3:
        park_week_3.append(item["durationPark"])
    elif int(item["week"]) == 4:
        park_week_4.append(item["durationPark"])
    else:
        park_week_5.append(item["durationPark"])


park_week_1=sorted(park_week_1)
park_week_2=sorted(park_week_2)
park_week_3=sorted(park_week_3)
park_week_4=sorted(park_week_4)
park_week_5=sorted(park_week_5)

book_week_1=sorted(book_week_1)
book_week_2=sorted(book_week_2)
book_week_3=sorted(book_week_3)
book_week_4=sorted(book_week_4)
book_week_5=sorted(book_week_5)

p1 = 1. * np.arange(len(park_week_1))/(len(park_week_1) - 1)
b1 = 1. * np.arange(len(book_week_1))/(len(book_week_1) - 1)
p2 = 1. * np.arange(len(park_week_2))/(len(park_week_2) - 1)
b2 = 1. * np.arange(len(book_week_2))/(len(book_week_2) - 1)
p3 = 1. * np.arange(len(park_week_3))/(len(park_week_3) - 1)
b3 = 1. * np.arange(len(book_week_3))/(len(book_week_3) - 1)
p5 = 1. * np.arange(len(park_week_5))/(len(park_week_5) - 1)
b5 = 1. * np.arange(len(book_week_5))/(len(book_week_5) - 1)
p4 = 1. * np.arange(len(park_week_4))/(len(park_week_4) - 1)
b4 = 1. * np.arange(len(book_week_4))/(len(book_week_4) - 1)



figWeek = plt.figure()
# plot cdf
plt.xscale("log")
plt.plot(park_week_1, p1, label="Book, w1", color="grey", alpha=0.8, linestyle="-")
plt.plot(book_week_1, b1, label="Park, w1", color="grey", alpha=0.8, linestyle="-.")
plt.plot(park_week_2, p2, label="Book, w2", color="blue", alpha = 0.8, linestyle="-")
plt.plot(book_week_2, b2, label="Park, w2", color="blue", alpha = 0.8, linestyle="-.")
plt.plot(park_week_3, p3, label="Book, w3", color="pink", alpha = 0.8, linestyle="-")
plt.plot(book_week_3, b3, label="Park, w3", color="pink", alpha = 0.8, linestyle="-.")
plt.plot(park_week_4, p4, label="Book, w4", color="red", alpha = 0.8, linestyle="-")
plt.plot(book_week_4, b4, label="Park, w4", color="red", alpha = 0.8, linestyle="-.")
plt.plot(park_week_5, p5, label="Book, w5", color="green", alpha = 0.8, linestyle="-")
plt.plot(book_week_5, b5, label="Park, w5", color="green", alpha = 0.8, linestyle="-.")


plt.legend()
plt.xlabel("Duration [min]")
plt.title( "CDF booking/parking duration in "+city+", October 2017", loc='center')
plt.grid(True, which='minor', axis="x")
plt.show()

fig = plt.figure()

park_duration = sorted(park_duration)
book_duration = sorted(book_duration)
p = 1. * np.arange(len(park_duration))/(len(park_duration) - 1)
b = 1. * np.arange(len(book_duration))/(len(book_duration) - 1)

plt.plot(park_duration, p, label="Book")
plt.plot(book_duration, b, label="Park")
plt.xscale("log")
plt.legend()
plt.xlabel("Duration [min]")
plt.title( "CDF booking/parking duration in "+city+", October 2017", loc='center')
plt.grid(True, which='minor', axis="x")
plt.show()

city= city.replace(" ", "")
figWeek.savefig("/images"+city+'Weeks.pdf')
fig.savefig("/images"+city+'.pdf')

