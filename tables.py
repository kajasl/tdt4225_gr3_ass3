from pprint import pprint 
from DbConnector import DbConnector
import os


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.ids = os.listdir("dataset/dataset/Data")
        self.labeled_ids = open("dataset/dataset/labeled_ids.txt", "r").read().splitlines()

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

        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
