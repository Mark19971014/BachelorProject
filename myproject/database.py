from abc import ABC, abstractclassmethod, abstractmethod

OBSERVATIONS = "observations"
NODES_INFO = "nodes_info"


class DataBase(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @property
    @abstractmethod
    def DATABASE(self):
        pass

    def find(self, collection, query):
        return self.DATABASE[collection].find(query)

    def find_one(self, collection, query, sort=None):
        return self.DATABASE[collection].find_one(query, sort=sort)

    def count(self, collection, query):
        return self.DATABASE[collection].count(query)
