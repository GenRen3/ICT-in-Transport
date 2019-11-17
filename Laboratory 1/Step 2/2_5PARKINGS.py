import pymongo as pm
import pprint as pp
import numpy as np
import matplotlib.pyplot as plt
import datetime, time
import os

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

first_oct_day = time.mktime((datetime.datetime(2017, 10, 1, 0, 0)).timetuple())
last_oct_day = time.mktime((datetime.datetime(2017, 11, 1, 0, 0)).timetuple())
folder = os.path.dirname(os.path.abspath(__file__))

step_time = {"city":["Milano", "Frankfurt", "New York City"], "time": [-3600,-3600,18000]}

city = "Frankfurt"
time_corrector = int(step_time["time"][step_time["city"].index(city)]) # correcting time to zone

percentile_value = 80
THlongPark = 7*24*60 # no more than 7 days stopped in the park (maybe gps problems? however car has problem)
THshortPark = 1*60 # no less han 1 minutes of rentals for parking
resultQuery = Car2goPermanentPark.aggregate([
    {"$match":  # stage 1 of the pipeline
         {"$and": [{"city": city},
                   {"init_time": {"$gte": first_oct_day + time_corrector}},
                   {"init_time": {"$lte": last_oct_day + time_corrector}}
                   ]}
    }, {"$project": {
        "_id": 0,
        "duration": {"$divide": [{"$subtract": ["$final_time", "$init_time"]}, 60]},
        "hourInDay": {"$hour": "$init_date"},
        "day": {"$dayOfMonth": "$init_date"},
        # "moved": {"$ne": [{"$arrayElemAt": ["$origin_destination.coordinates", 0]},
        #                   {"$arrayElemAt": ["$origin_destination.coordinates", 1]}]}
        }
    },{"$match":  # stage 1 of the pipeline
                              {"$and": [#{"moved": True},
                                        {"duration": {"$gte": THshortPark}},
                                        {"duration": {"$lte": THlongPark}}
                                        ]}
    }, {"$sort": {
          "day": 1,
          "hourInDay": 1
          }
     }
])
booking = {}
avg = []
med = []
std = []
percentile = []
for item in resultQuery:
    day = item["day"]
    durationBooking = item["duration"]
    if day not in booking:
        booking[day] = []
    booking[day].append(float(durationBooking))

for key in booking.keys():
    value = booking[key]
    avg.append(np.mean(value))
    med.append(np.median(value))
    std.append(np.std(value))
    percentile.append(np.percentile(value, percentile_value))

fig = plt.figure(1, figsize=(10, 5))
plt.title("Stats in "+city)
plt.xlabel("October 2017")
plt.ylabel("Parkings duration")
plt.xlim(1, 31)
x = np.linspace(1, 31, len(avg))
plt.grid(True)
plt.plot(x, avg, label="Avg")
plt.plot(x, med, label="Med")
plt.plot(x, std, label="Std")
plt.plot(x, percentile, label= str(percentile_value)+"percentile")
plt.xticks(np.arange(32))
plt.legend(loc=2)
fig.show()
#fig.savefig(folder + '/med' + city + '.png')

figError = plt.figure(2, figsize=(10, 5))
mean_plus = []
mean_minus = []

for el_avg, el_std in zip(avg, std):
    mean_plus.append(el_avg+el_std)
    mean_minus.append(el_avg-el_std)

plt.title("Error in " + city)
plt.xlabel("October 2017")
plt.ylabel("Parkings duration")
plt.xlim(1, 31)
x = np.linspace(1, 31, len(avg))
plt.grid(True)
plt.plot(x, avg, label="Avg")
plt.plot(x, mean_plus, label="avg+std")
plt.plot(x, mean_minus, label="avg-std")
plt.xticks(np.arange(32))
plt.legend(loc=2)
fig.show()
city = city.replace(" ", "")
figError.savefig(folder + '/PARK'+city+'_error(2_5).pdf')
fig.savefig(folder + '/PARKstats'+city+'(2_5).pdf')



