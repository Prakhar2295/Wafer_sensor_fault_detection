import sqlite3
import shutil
from datetime import datetime
import csv
import os
from application_logging.logger import App_Logger

class dboperation:

    """
        This class will be used to store the good_raw data in the table which is 
        located in the databases inside the database.This class will also be used to fetch all 
        data from the database stored in the form of data.This will handle all SQL operation.

        Written By: JSL
        Version: 1.0
        Revisions: None

    """

    def __init__(self):
        self.path = 'Training_Database/'
        self.badFilePath = "Training_RAW_files_validated/Bad_Raw"
        self.goodfile = "Training_RAW_files_validated/Good_Raw"
        self.logger = App_Logger()


    def dataBaseConnection(self,DataBaseName):
        """
            Method Name: dataBaseConnection
            Description: This database will be used to create the database and if it already exists it
            will make a connection to the database.

            Output: Connection to the DB
            On Failure: Raise ConnectionError


            Written By: JSL
            Version: 1.0
            Revisions: None

        """ 

        try:
            conn = sqlite3.connect(self.path + DataBaseName+ '+.db')

            file = open("Training_Logs/DataBaseConnectionLog.txt",'a+')
            self.logger.log(file,"Opended %s database successfully" %DataBaseName)
            file.close()
        except ConnectionError:
            file = open("Training_Logs/DataBaseConnectionLog.txt",'a+')
            self.logger.log(file,"Error occurred while connecting with with database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn

     





