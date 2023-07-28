from datetime import datetime
import pandas
from os import listdir
from application_logging.logger import App_Logger



class dataTransform:

    """
        This class will be used to transform the Good Raw Training data before inserting into the database table.

        
        Written by: JSL
        version: 1.0
        Revisions: None
    
    """

    def __init__(self):
        self.goodDataPath = "Training_Raw_files_validated/Good_Raw"
        self.logger = App_Logger()


    def replaceMissingWithNull(self):

        """
             Method Name: replaceMissingWithNull
             Description: This method reaplces the missing value in the good raw data 
             with "NULL", so that this data can be easily inserted into database.
             This method also takes the substring of the first column i.e. "INTEGER", for ease up the loading process in the 
             in database.This columns is anyways going to be be removed during training.

            Output: None
            On failure: exception
            
            Written by: JSL
            version: 1.0
            Revisions: None

        """ 

        log_file = open("Training_Logs/dataTransformLog.txt", 'a+')

        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                csv = pandas.read_csv(self.goodDataPath + "/" + file)
                csv.fillna("NULL",inplace = True)

                csv["wafer"] = csv["wafer"].str[6:]
                csv.to_csv(self.goodDataPath + "/" + file,index = None,header= True)
                self.logger.log(log_file,"%s: File Transformed succesfully!!" %file)
                

        except Exception as e:
            self.logger.log(log_file,"Data Transformation failed: %s "%e)
            log_file.close()

        log_file.close()    





                    



