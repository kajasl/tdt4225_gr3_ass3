from pprint import pprint 
from DbConnector import DbConnector
from datetime import datetime, timedelta
from bson.objectid import ObjectId
import os



class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.ids = os.listdir("dataset/dataset/Data")
        self.labeled_ids = open(r"dataset\dataset\labeled_ids.txt", "r").read().splitlines()

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_users(self):
        docs= []
        for iden in self.ids:
            value = False
            for label_id in self.labeled_ids:
                if label_id == iden:
                    value = True
            if(len(iden)<4): 
                docs.append({"_id": iden, "has_labels": value})
        collection = self.db["User"]
        collection.insert_many(docs)
    

    def insert_activities_and_trackpoints (self, user_id, transportation, activity_id_start):

        path = 'dataset/dataset/Data/'+ user_id + '/Trajectory'

        #to keep incrementing activity id  for trackpoints
        activity_id = activity_id_start
        activity_docs=[]
        batch_trackpoints = []
        for (root, dirs, files) in os.walk(path):
            for fil in files:

                trackpoints_list = open(path + '/' + fil).read().splitlines()
                if len(trackpoints_list) <=2506:

                    start_date = (trackpoints_list[6].split(',')[5] +" "+ trackpoints_list[6].split(',')[6]).replace('-','').replace(':','').replace(' ','')
                    end_date = (trackpoints_list[-1].split(',')[5] +" "+ trackpoints_list[-1].split(',')[6]).replace('-','').replace(':','').replace(' ','')
                    
                    #couldve used NULL for database functionality
                    mode = 'none'
                    #a bit heavy to go through entire labels.txt every time to check if transportation mode should be added to activity
                    if transportation != 'NULL':
                        start_end_date = start_date + end_date
                        #check if activity should be labeled a transportation mode. Works only for transportation modes that last the entire activity
                        if start_end_date in transportation:
                            mode = transportation[start_end_date]
                    
                    activity_docs.append({"_id":activity_id, "user_id": user_id, "transportation_mode": mode, "start_date_time": start_date, "end_date_time": end_date})

                    #should be trackpoints_list[6:] but only a few lines so execution is fast
                    trackpoints_stripped = trackpoints_list[6:]

                    #list to hold trackpoint information to be able to executemany
                    
                    for trackpoint in trackpoints_stripped:

                        trackpoint_line = trackpoint.split(',')

                        latitude = (trackpoint_line[0])
                        longitude = (trackpoint_line[1])
                        altitude = (trackpoint_line[3])
                        days = (trackpoint_line[4])
                        date = (trackpoint_line[5] + trackpoint_line[6]).replace('-','').replace(':','').replace(' ','')

                        batch_trackpoints.append({"activity_id":activity_id, "lat": latitude, "lon":longitude, "altitude": altitude, "date_days": days, "date_time": date})

                    activity_id += 1
        # Avoid adding activities when none meets conditions
        if(activity_docs!=[]):
            collection = self.db["Activity"]
            collection.insert_many(activity_docs)

            collection = self.db["TrackPoint"]
            collection.insert_many(batch_trackpoints)
        return activity_id

    def transportation (self, user_id):
        collection = self.db["User"]
        label = collection.find_one({"_id": user_id}, {"_id": 0, "has_labels": 1})
        transportation = 'NULL'
        if label == {'has_labels': True}:
            mode = open ('dataset/dataset/Data/' + user_id +'/labels.txt', 'r')
            transportation_mode = mode.read().splitlines()
            #pop redundant info first line
            transportation_mode.pop(0)

            transportation = {}

            for activity in transportation_mode:
                transp_mode = activity.split('\t')
                label_start_end_date = transp_mode[0].replace('/','').replace(':','').replace(' ','') + transp_mode[1].replace('/','').replace(':','').replace(' ','')
                transportation.update( {label_start_end_date : transp_mode[2] } )
        return transportation


    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['assignment3'].list_collection_names()
        print(collections)

    def update_to_iso_date(self):
        collection = self.db["Activity"]
        collection.update_many({}, [{
            '$set':
                {
                    'start_date_time':
                        {
                            '$dateFromString':
                                {
                                    'dateString': '$start_date_time',
                                    'format': '%Y%m%d%H%M%S'
                                }
                        },
                    'end_date_time':
                        {
                            '$dateFromString':
                                {
                                    'dateString': '$end_date_time',
                                    'format': '%Y%m%d%H%M%S'
                                }
                        }
                }
        }])

        collection2 = self.db['TrackPoint']
        collection2.update_many({}, [{
            '$set':
                {
                    'date_time':
                        {
                            '$dateFromString':
                                {
                                    'dateString': '$date_time',
                                    'format': '%Y%m%d%H%M%S'
                                }
                        }
                }
        }])


    #task 1
    def first_ten_rows(self):
        collection1 = self.db["User"].find({}).limit(10)
        collection2 = self.db["Activity"].find({}).limit(10)
        collection3 = self.db["TrackPoint"].find({}).limit(10)
        print("Users ----------->")
        for doc in collection1: 
            print(doc)
        print("Activities ----------->")
        for doc in collection2: 
            print(doc)
        print("Trackpoints ----------->")
        for doc in collection3: 
            print(doc)

    #task 2.1
    def count_db(self):
        activities = self.db["Activity"]
        users = self.db["User"]
        trackpoints = self.db["TrackPoint"]

        users_count = users.count()
        activities_count = activities.count()
        trackpoints_count = trackpoints.count()

        print("There are",users_count,"users,",activities_count,"activities and",trackpoints_count,"trackpoints inserted in the database.")


    #task 2.2
    def average_activities(self):
        activities = self.db["Activity"]
        users = self.db["User"]

        activities_count = activities.find({}).count()
        users_count = users.find({}).count()
        print("Average activities for a user is: " + str(round(activities_count / users_count)))

    #task 2.3
    def find_users_with_most_activities(self):
        activities = self.db["Activity"]
        top_users = activities.aggregate([
            { '$unwind': "$user_id" },  { '$sortByCount': "$user_id" }, {'$limit': 20}
        ]
        )
        for user in top_users:
            print(user)  

    #task 2.4
    def find_taxi_users(self):
        activity_table = self.db["Activity"]
        taxi_activities = activity_table.aggregate([
            {'$match': {'transportation_mode': 'taxi'} },
            {'$group': {'_id': '$user_id'} },
            {'$sort': {'_id': 1} }
        ])
        print("Taxi users:")
        for activity in taxi_activities:
            print(activity)

    #task 2.6a
    def find_year_most_activities(self):
        activity_years = self.db["Activity"].aggregate([
            {
                '$group': 
                    {
                        '_id': {'$year': '$start_date_time'},
                        'count': {'$sum' : 1}
                    }
            },
            {
                '$sort':
                    {
                        'count': -1
                    }

            },
            {
                '$limit': 1
            }
            
        ])
        print("year with most activities:")
        for activity_year in activity_years:
            pprint(activity_year)

    #task 2.6b
    def find_year_most_activities_hours(self):
        activity_years = self.db["Activity"].aggregate([
            {
                '$addFields':
                    {
                        'hours':
                            {
                                '$divide':
                                    [
                                        {'$subtract': ['$end_date_time', '$start_date_time']},
                                        3600000
                                    ]
                            }
                    }
            },
            {
                '$group': 
                    {
                        '_id': {'$year': '$start_date_time'},
                        'sum_hours':
                            {
                                '$sum': '$hours'
                            }
                    }
            },
            {
                '$sort':
                    {
                        'sum_hours': -1
                    }

            }
            
        ])
        print("years with most activity hours descending:")
        for activity_year in activity_years:
            pprint(activity_year)

    #finds all transportation activities with transportationmodes != none
    def findtransp(self):
        activity_table = self.db["Activity"].aggregate([
            {'$match': {'transportation_mode': {'$ne': 'none'} } }
        ])
        
        for activity in activity_table:
            pprint(activity)

    #task 2.9        
    def find_invalid_activities(self):
        trackpoints = self.db["TrackPoint"]
        activities = trackpoints.aggregate([
            {'$group': {
                '_id':'$activity_id',
                'timeArr': {'$push':'$date_time'}
                }
            }
        ],allowDiskUse=True)



        invalids = []
        for activity in activities:
            trackpoints = activity['timeArr']
            for i in range(1,len(trackpoints)):
                time_diff = (trackpoints[i-1]-trackpoints[i]) / timedelta(minutes=1)
                if(time_diff < 5):
                    invalids.append(activity["_id"])
                    break
        activities = self.db["Activity"]
        
        invalid_users = activities.aggregate([
            {'$match': {'_id': {'$in': invalids}}},
            {'$group': {
                '_id': "$user_id",
                'count': { '$sum': 1 }

            }}
        ]).count()

        print(invalid_users)        

    #task 2.10
    def users_in_forbidden_city(self):
        trackpoints = self.db["TrackPoint"]
        users = trackpoints.aggregate([
            {'$project': {
                '_id': 1,
                'user_id': 1,
                'activity_id': 1,
                'lat': {'$round': [{'$toDouble':'$lat'},3]},
                'lon': {'$round': [{'$toDouble':'$lon'},3]}  
            }
            },
            {'$match': {'$and': [{'lat': 39.916}, {'lon': 116.397}]}},
            {'$lookup': {
                'from':'Activity',
                'localField': 'activity_id',
                'foreignField':'_id',
                'as':'activity_id' 
            }}
        ])

        for user in users:
            pprint(user)


def main():
    program = None
    try:
        program = ExampleProgram()    
        # Code to insert data into db    
        # program.create_coll(collection_name="User")
        # program.create_coll(collection_name="Activity")
        # program.create_coll(collection_name="TrackPoint")
        # program.insert_users()
        # activity_id = 1
        # for id in program.ids:
        #     print("id changed ------------------------->", id)
        #     transp = program.transportation(user_id = id)
        #     activity_id = program.insert_activities_and_trackpoints(user_id = id, transportation = transp, activity_id_start = activity_id)
        #program.update_to_iso_date()
        #task 1
        # program.first_ten_rows()

        #task 2.1
        #program.count_db()

        #task 2.2
        #program.average_activities()
        
        #task 2.3
        program.find_users_with_most_activities()
        
        #task 2.4
        # program.find_taxi_users()

        #task 2.6
        # program.find_year_most_activities()
        # program.find_year_most_activities_hours()
        

        #task 2.8

        #task 2.9
        #program.find_invalid_activities()

        #task 2.10
        #program.users_in_forbidden_city()

        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
