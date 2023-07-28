import sqlite3
import shutil
from datetime import datetime
import csv
from os import listdir
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
    
    def createTableDb(self,DataBaseName,column_names):
        """
            Method Name: createTableDb
            Description: This method is used to set create the table if not exists
            and if already exists it will alter the table.

            Output: Connection to the DB
            On Failure: Raise ConnectionError


            Written By: JSL
            Version: 1.0
            Revisions: None  
        
        """
        try:
            conn = self.dataBaseConnection(DataBaseName)
            c = conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'Good_Raw_Data'")
            if c.fetchone()[0] == 1:
                conn.close()
                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file,"Tables Created successfully !!")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file,"Cloased %s database successfully !!" %DataBaseName)

            else:

                for key in column_names.keys():
                    type = column_names[key]

                    #in try block we check if the table exists, if yes then add columns to the table
                    # else in catch block we will create the table


                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name = key,dataType = type))

                    except:
                        conn.exceute('CREATE TABLE Good_Raw_Data ({column_name} {dataType})'.format(column_name = key,dataType = type))

                conn.close()

                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file,"Tables created successfully")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file,"closed %s database successfully" %DataBaseName)
                file.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file,"Error while creating table:: %s"  %e)
            file.close()
            conn.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file,"Closed %s database successfully " %DataBaseName)
            file.close()
            raise e
        

    def insertIntotableGoodData(self,DataBase):
        """
                Method Name: insertIntoTableGoodData
                Description: This method inserts the Good data files from the Good_Raw folder into the
                            above created table.
                Output: None
                On Failure: Raise Exception

                Written By: JSL
                Version: 1.0
                Revisions: None
               
        """  
        conn = self.dataBaseConnection(DataBase)
        goodFilepath = self.goodFilePath
        badFilepath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilepath)]
        #log_file = open("Training_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:
                with open(goodFilepath+ '/'+file,'r') as f:
                    next(f)
                    reader = csv.reader(f,delimiter = "\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values = ( list_)))
                                log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
                                self.logger.log(log_file,"%s: File loaded successfully !!" %file)
                                log_file.close()
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:
                conn.rollback()
                log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
                self.logger.log(log_file,"Error while inserting data into table:: %s" %e)
                shutil.move(goodFilepath +'/' + file,badFilepath)
                self.logger.log(log_file,"File Moved successfully %s" %file)
                log_file.close()
                conn.close()
                

        conn.close()        




                   

     





