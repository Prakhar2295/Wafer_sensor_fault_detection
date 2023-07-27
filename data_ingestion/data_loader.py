import pandas as pd
#from application_logging import logger



class Data_Getter:

    """
    This class shall be used to load the training data from the source.

    Written by: JSL
    Version: 1.0
    Revisions: None

    """
    def __init__(self,file_object,logger_object):
        self.training_file = "Training_FileFromDB/INPUTfile.csv"
        self.file_object = file_object
        self.logger_object = logger_object

    def get_data(self):

 
        """
        Method Name: get_data
        Description: This method will be used for reading the data from the source.
        Output: A pandas DataFrame
        On failure: Raise Exception

        Written by: JSL
        Version: 1.0
        Revisions: None

        """
        self.logger_object.log(self.file_object,"Entered the get_data method of the Data Gtter class")
        try:
            self.data = pd.read_csv(self.training_file)  ###reading the training data
            self.logger_object.log(self.file_object,"Data Load Successfull.Exited the get_data method of the Data Getter class")

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_data method of the Data Getter class:' + str(e))
            self.logger_object.log(self.file_object,"Data Load unsuccessfull.Exited the get_data method of the Data Getter class")

            raise Exception()













        


   