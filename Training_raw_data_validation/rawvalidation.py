import sqlite3
import pandas as pd
import os
from os import listdir
import re
import json
import shutil
from application_logging.logger import App_Logger
from datetime import datetime



class Raw_Data_Validation:


    """
         This class be used used to handling all the validation done on the raw training data.

         Written by: JSL
         Version: 1.0
         Revisions: None
    
    """

    def __init__self(self,path):
        self.Batch_directory = path
        self.schema_path = "schema_training.json"
        self.logger = App_Logger()

    def valuesFromSchema(self):


        """
        Method Name: ValuesFromSchema
        Description: This method will extract all the relevant information from the pre defined schema.
        Output: LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,NumberofColumns
        On Failure: KeyError,valueError,Exception

        Written By: JSL
        Version: 1.0
        Revisions: None
        
        """

        try:
            with open(self.schema_path,'r') as f:
                dic = json.load(f)
                f.close()
                pattern = dic["SampleFileName"]
                LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]
                LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]
                column_names = dic["ColName"]
                NumberofColumns = dic["NumberofColumns"]
                file = open("Training_Logs/valuesfromSchemavalidationLogs.txt",'a+')
                log_message = "LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" %LengthOfTimeStampInFile + "\t" + "NumberofColumns:: %s" %NumberofColumns + "\t" + "Filename:: %s" %pattern + "\n"
                self.logger.log(file,log_message)

                file.close()

        except ValueError:
            file = open("Training_Logs/valuesfromSchemavalidationLogs.txt",'a+')
            self.logger.log(file,"ValueError: Value not found inside schema_taining.json")
            file.close()
            raise ValueError
        
        except KeyError:
            file = open("Training_Logs/valuesfromSchemavalidationLogs.txt",'a+')
            self.logger.log(file,"KeyError: Key value error incorret key passes")
            file.close()

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemavalidationLogs.txt",'a+')
            self.logger.log(file,str(e))
            file.close()
            raise e
        
        return LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,NumberofColumns
    
    def manualregexcreation(self):

        """
           Method Name: manualregexcreation
           Description: This method will be used to identify the raw data file names as per the training schema.
           Ouput:Regex Pattern
           On failure: None

           Written By: JSL
           Version: 1.0
           Revisions: None



        
        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"

        return regex
    

    def createDirectoryforGoodBadRawData(self):

        """
            Method Name: createDirectoryForGoodBadRawData
            Description: This method is used to create directories for raw data which is good or bad.

            Output: Directory for good or bad data
            On failure: OS error

             Written By: JSL
             Version: 1.0
             Revisions: None

        """  
        try:
            path = os.path.join("Training_RAW_files_validated/","Good_Raw/")
            if not os.path.isdir(path):
                os.mkdir(path)
            path = os.path.join("Training_RAW_files_validated/","Bad_Raw/")
            if not os.path.isdir(path):
                os.mkdir(path)

        except OSError as ex:
            file = open("Training_Logs/General_log.txt",'a+')
            self.logger.log(file,"Error ocurred while creating directory:: %s" %ex)
            file.close()
            raise OSError
        
    def deleteExistingGoodDataTrainingFolder(self):

        """
            Method Name: deleteExistingGoodDataTrainingFolder
            Description: Delete the existing Good data training folder after moving the data 
            to the DB table, to ensure the space optimization thse good data training directories are deleted.

            Output:None
            On failure: OS error

            Written By: JSL
            Version: 1.0
            Revisions: None

        """ 
        try:
            path = "Training_RAW_files_validated/"
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path + "Good_Raw/")
                file = open("Training_Logs/General_log.txt",'a+')
                self.logger.log(file,"Good Raw directory has been deleted succesfully!!!")
        
        except OSError as s:
            file = open("Training_Logs/General_log.txt",'a+')
            self.logger.log(file,"OS error ocurred in deleting good raw training directory:: %s" %s)
            raise OSError
        







                 
        
                


















    