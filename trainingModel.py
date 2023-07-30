"""

This is the entry point for Training the Machine Learning Model.

Written By: JSL
Version: 1.0
Revision: None


"""


###Doing the necessary imports

from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger


class trainmodel:


    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt",'a+')

    def trainingmodel(self):
        ###Logging the start of the training
        self.log_writer.log(self.file_object,"Start Of Training")
        try:
            ###Getting the data from the source
            data_getter =  data_loader.Data_Getter(self.file_object,self.log_writer)
            data = data_getter.get_data()

            ###Doing the data preprocessing
        except:
            pass    
                 


