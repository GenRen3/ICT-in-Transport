import pymongo as pm
import pprint as pp
import numpy as np
import matplotlib.pyplot as plt
import datetime, time
import os
import datetime

#Here we set the client
client = pm.MongoClient('bigdatadb.polito.it', ssl=True,
         authSource = 'carsharing', tlsAllowInvalidCertificates=True)

#Here we access to a specific collection of the client
db = client['carsharing']
db.authenticate('ictts', 'Ictts16!', mechanism='SCRAM-SHA-1') #authentication

#Collection for Car2go
permanentBook = db['PermanentBookings']
permanentPark = db['PermanentParkings']

# first week of october
init = time.mktime((datetime.datetime(2017, 10, 1, 0, 0)).timetuple())
last = time.mktime((datetime.datetime(2017, 10, 8, 0, 0)).timetuple())
folder = os.path.dirname(os.path.abspath(__file__))

step_time = {"city":["Milano", "Frankfurt", "New York City"], "time": [-3600,-3600,18000]}
city = "Frankfurt"
time_corrector = int(step_time["time"][step_time["city"].index(city)]) # correcting time to zone

THlongBook = 3*60 # 3hours
THshortBook = 5 # 5 min

Milano_dict = {}
frank_dict = {}
NYC_dict = {}
for city in step_time["city"]:
    time_corrector = int(step_time["time"][step_time["city"].index(city)])
    result = permanentBook.aggregate([
        {"$match":  # stage 1 of the pipeline
             {"$and": [{"city": city}, {"init_time": {"$gte": init + int(time_corrector)}},
                       {"init_time": {"$lt": last + int(time_corrector)}}]}

        },
        {"$project": {
            "_id": 0,
            "durationBook": {"$divide": [{"$subtract": ["$final_time", "$init_time"]}, 60]},
            "ok_duration": { "$and": [{"durationBook": {"$gt": ["$durationBook", THshortBook]}}, {"durationBook": {"$lte": ["$durationBook", 180]}}] },
            "hourUnix": {"$floor": {"$divide": ["$init_time", 3600]}},
            "moved": {"$ne": [{"$arrayElemAt": ["$origin_destination.coordinates", 0]},
                              {"$arrayElemAt": ["$origin_destination.coordinates", 1]}]},
            #"day": {"$dayOfMonth": "$init_date"},
            "week_day": {"$dayOfWeek": "$init_date"}
            }
        },{"$match":
            {"moved": True,
             "ok_duration": True
             }
           },
        {"$group": { # group for days!
            #"_id": {"day": "$day"},
            "_id": {"day": "$week_day"},

            "totalBooking": {"$sum": 1}},
            # "average_duration": {"$avg": "$durationBook"},
            # "average_amount": {"$avg": "$totalBooking"},
            # "median_":{}, # median for each day of the collection
            # "standard_deviation": {}, # std for each day of the colletion --> annotate.plt
            # "percentil":{} # percentil for each day of the collection
         }
        ,{"$sort": {
              "_id.day": 1
              }
        }
    ])
    booking = {}
    for item in result:
        days = item["_id"]["day"]
        totalBookingCount = item["totalBooking"]
        booking[days] = totalBookingCount

    if city == "Milano":
        Milano_dict = booking
    elif city == "Frankfurt":
        frank_dict=booking
    else:
        NYC_dict= booking


week = ["Mondays", "Tuesdays", "Wednesdays", "Thursdays", "Fridays", "Saturdays", "Sundays"]

fig = plt.figure(1, figsize=(20, 10))
# x = np.linspace(1, 7, len(booking))
plt.xticks(list(Milano_dict.keys()), week)
plt.grid(True, which='both')
plt.plot(list(Milano_dict.keys()), list(Milano_dict.values()), label="Milano")
plt.plot(list(Milano_dict.keys()), list(NYC_dict.values()), label="NYC")
plt.plot(list(Milano_dict.keys()), list(frank_dict.values()), label="Frankfurt")

plt.xlabel('Day')
plt.ylabel('Number of Bookings')
plt.title(' Bookings per day of week' + city)
plt.legend()
city = city.replace(" ", "")
fig.savefig(folder + '/weekAllCities.pdf')

plt.show()