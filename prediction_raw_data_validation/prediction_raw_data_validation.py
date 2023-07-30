import os
from datetime import datetime
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger



class prediction_data_validation:
    """
               This class shall be used for handling all the validation done on the Raw Prediction Data!!.

               Written By: JSL
               Version: 1.0
               Revisions: None

               """

    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger = App_Logger()


    def values_from_schema(self):
        """
                                Method Name: values_from_schema
                                Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                                Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                                On Failure: Raise ValueError,KeyError,Exception

                                Written By: JSL
                                Version: 1.0
                                Revisions: None

                                        """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("prediction_logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()



        except ValueError:
            file = open("prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns
    
    def regex_creation(self):
        """
              Method: regex_creation
              Description: This method will create a new regex which will be used 
              to validate the prediction file names.

              Output: Regex
              On Failure: None

              Written By: JSL
              Version: 1.0
              Revisions: None
 
        
        """
        regex = "[\wafer]+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createdirectoryforGoodBadpredictiondata(self):

        """

             Method Name: createdirectoryforGoodBadpredictiondata
             Description: This method is used to create directories for good and bad prediction data.
             Output: Directory
             On failure: OS Error,Exception

              Written By: JSL
              Version: 1.0
              Revisions: None


        """
        try:
            file = open("prediction_logs/General_logs.txt",'a+')
            self.logger.log(file,"Entered inside the createdirectoryforGoodBadpredictiondata method inside prediction raw data class")
            file.close()
            Good_data_path = "Raw_prediction_data/Good_data"
            Bad_data_path = "Raw_prediction_data/Bad_data"

            if not os.path.isdir(Good_data_path):
                os.makedirs(Good_data_path)

            if not os.path.isdir(Bad_data_path):
                os.makedirs(Bad_data_path)
            file = open("prediction_logs/General_logs.txt",'a+')
            self.logger.log(file,"Created Directory for good data and bad data:: %s " %Good_data_path ,"Bad Data %s" %Bad_data_path)
            file.close()
        except OSError:
            file = open("prediction_logs/General_logs.txt",'a+')
            self.logger.log(file, "Error Occurred while creating directory %s" %OSError )
            raise OSError

    def deletedirectoryforGooddata(self):
        """
              Method Name:  deletedirectoryforGooddata
              Description: This method will be used to delete the good data directory
              after moving the data to prediction db for space optimization.

              Output:None
              On Failure: OS Error

              Written By: JSL
              Version: 1.0
              Revisions: None

        """
        file = open("prediction_logs/General_logs.txt",'a+')
        self.logger.log(file,"Entered inside deletedirectoryforGooddata inside prediction_raw_data class")
        file.close()

        #Bad_data_path = "Raw_prediction_data/Bad_data"
        try:
            Good_data_path = "Raw_prediction_data/Good_data"
            if os.path.isdir(Good_data_path):
                shutil.rmtree(Good_data_path)

                file = open("prediction_logs/General_logs.txt", 'a+')
                self.logger.log(file, "Deleted good data directory Successfully!!")
                file.close()

        except OSError as e:
            file = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.log(file, "Error Occurred while deleting the good data dat directory.Exception Message::" + str(e))
            file.close()
            raise OSError


def deletedirectoryforBaddata(self):
    """
          Method Name:  deletedirectoryforBaddata
          Description: This method will be used to delete the bad data directory
          after moving the data to prediction db for space optimization.

          Output:None
          On Failure: OS Error

          Written By: JSL
          Version: 1.0
          Revisions: None

    """
    file = open("prediction_logs/General_logs.txt", 'a+')
    self.logger.log(file, "Entered inside deletedirectoryforBaddata inside prediction_raw_data class")
    file.close()
    #Good_data_path = "Raw_prediction_data/Good_data"

    try:
        Bad_data_path = "Raw_prediction_data/Bad_data"
        if os.path.isdir(Bad_data_path):
            shutil.rmtree(Bad_data_path)

            file = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.log(file, "Deleted bad data directory Successfully!!")
            file.close()

    except OSError as e:
        file = open("prediction_logs/General_logs.txt", 'a+')
        self.logger.log(file, "Error Occurred while deleting the good data dat directory.Exception Message::" + str(e))
        file.close()
        raise OSError

















    





            
        
            
            
           
		
        
	
	   

	












   
	
	
	

		

	
	   

	












