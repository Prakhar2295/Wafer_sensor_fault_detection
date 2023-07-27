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
        pass
                


















    