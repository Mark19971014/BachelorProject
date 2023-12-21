import pymongo
import time

from myproject.database import DataBase
from myproject.database import OBSERVATIONS


class DatabaseManager(DataBase):
    def __init__(self,
                 name: str = 'mySDN',
                 host: str = 'localhost',
                 port: int = 27017
                 ):
        self.name = name
        self.host = host
        self.port = port
        self.URI = "mongodb://" + host + ":" + str(port)
        self.client = None
        self.DATABASE = None

    def initialize(self):
        self.client = pymongo.MongoClient(self.URI)
        self.DATABASE = self.client[self.name]


    def DATABASE(self):
        return self.DATABASE

    # Functions for observation
    def getNumberOfObservations(self):
        db = self.find(OBSERVATIONS, {})
        count = 0
        for _ in db:
            count += 1

        return count

    def get_power_avg(self, SkipSteps=0, no_elements=5):

        db = self.find_one(OBSERVATIONS, {})
        if db is None:
            return None

        db = self.find(
            OBSERVATIONS, {}).skip(SkipSteps).limit(no_elements)

        summation = []

        for doc in db:
            power_avg = doc["power_avg"]

            summation.append(power_avg)
            
            ts = doc["timestamp"]
            #do not covert ts if we read the data in local realtime
            ts = int(round(time.time()*1000))

        PowerAvg = sum(summation) / len(summation)

        return {'ts': int(ts),
                "values": {'power': PowerAvg}}
      

    def get_delay_avg(self, SkipSteps=0, no_elements=5):
        db = self.find_one(OBSERVATIONS, {})
        if db is None:
            return None
        db = self.find(
            OBSERVATIONS, {}).skip(SkipSteps).limit(no_elements)
        Total_Delay = []

        for doc in db:
            delay_avg = doc["delay_avg"]

            Total_Delay.append(delay_avg)

            ts = doc["timestamp"]
            #convert the time to local realtime
            ts = int(round(time.time()*1000))
        DelayAvg = sum(Total_Delay) / len(Total_Delay)

        return {'ts': int(ts),
                "values": {'delay': DelayAvg}}
        # return {'delay': DelayAvg}

    def get_pdr_mean(self, SkipSteps=0, no_elements=5):
        db = self.find_one(OBSERVATIONS, {})
        if db is None:
            return None
        db = self.find(
            OBSERVATIONS, {}).skip(SkipSteps).limit(no_elements)
        Total_pdr = []

        for doc in db:
            delay_avg = doc["pdr_mean"]
            ts = doc["timestamp"]
            ts = int(round(time.time()*1000))
            Total_pdr.append(delay_avg)

        Pdr_mean = sum(Total_pdr) / len(Total_pdr)

        return {'ts': int(ts),
                'values': {'pdr_mean': Pdr_mean}}
        # return {'pdr_mean': Pdr_mean}

