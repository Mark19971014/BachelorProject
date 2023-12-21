import sys
from myproject.db_manger import DatabaseManager
import myproject.mqtt as mqtt
import threading
import os
from myproject.Node_manager import *
from myproject import Node_manager
from myproject.database import *
import time

threadPool = []


def threadRunner(function, args1):
    t = threading.Thread(target=function, args=args1)
    t.start()
    threadPool.append(t)


def ObservationManager(my_db):
    doc_num = my_db.getNumberOfObservations()
    i = 1
    while True:
        # check if enough 5 data
        while doc_num < (5 * i):
            # get the current number of documnet in DB
            doc_num = my_db.getNumberOfObservations()

        powerAvg = my_db.get_power_avg(i * 5, 5)
        mqtt.publish_PowerAvg(powerAvg)

        delayAvg = my_db.get_delay_avg(i * 5, 5)
        mqtt.publish_DelayAvg(delayAvg)

        pdr_mean = my_db.get_pdr_mean(i * 5, 5)
        mqtt.publish_Pdr_mean(pdr_mean)

        time.sleep(5)
        i += 1


def NodesManager(my_db):
    Nodes_Array = []
    while True:
        db = my_db.find_one(NODES_INFO, {})
        if db is None:
            return None

        db = my_db.find(NODES_INFO, {})

        for doc in db:
            # check if the nodes already existed
            if not (doc in Nodes_Array):
                # append the node to the array if it is the first time accessed
                Nodes_Array.append(doc)
                node_obj = Nodes(node=doc)
                # Thread of initilizing the Node
                threadRunner(Node_manager.run, (node_obj,))


def main():
    # create a DatabaseManager class object
    my_db = DatabaseManager()
    # initialize the database
    my_db.initialize()
  
    # Thread of observation
    threadRunner(ObservationManager, (my_db,))
    # Thread of Nodes_info
    threadRunner(NodesManager, (my_db,))

    key = ""
    while key != "x":
        key = input("press x to exit\n")
    os._exit(0)


if __name__ == '__main__':
    main()

    sys.exit(0)
