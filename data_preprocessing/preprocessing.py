import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer

class prepocessor:

    """
        This class will be used to clean & transform the data befor training.

        Written By: JSL
        Version: 1.0
        Revisions: None
        
    """
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object


    def remove_columns(self,data,columns):

        """
             Method Name: remove_columns
             Description: This method removes the given columns from the pandas dataframe.
             Output: A dataframe after removing the specified dataframe.
             On failure: An Exception is raised


             Written By: JSL
             Version: 1.0
             Revisions: None  
        
        """

        self.logger_object.log(self.file_object,"Entered the remove column method of the preprocessing class")
        self.data = data
        self.columns = columns
        try:
            self.useful_data = self.data.drop(label = self.columns,axis = 1)  # drop the labels specified in the columns
            self.logger_object.log(self.file_object,"Column Removal Successfull it's names are:: %s" %self.columns)

            return self.useful_data

        except Exception as e:
            self.logger_object.log(self.file_object,"Error occurred while removing columns it's names are:: %s" %self.columns)
            self.logger_object.log(self.file_object,"Error occurred while removing %s" %e)

            raise Exception()
        
    def separate_label_features(self,data,label_column_name):

        """
                  Method: separate_label_features
                  Description: This method separates the features and the label column.
                  Output: Returns two separate dataframes, one containing the features and the other containing labels.
                  On failure : Raises Exception


                  Written By: JSL
                  Version: 1.0
                  Revisions: None 

        """

        self.logger_object.log(self.file_object,"Entered the separate the label feature method inside the preprocessing class")
        try:
            self.X = data.drop(label = label_column_name,axis = 1)  ###dropping the label column from the dataframe

            self.Y = data[label_column_name]  ##Assigning the label column to the Y dataframe.

            self.logger_object(self.file_object,"Exiting the separate label features method")

            return self.X,self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,"Exception occurred during separating the dataframe %s"%e)
            self.logger_object.log(self.file_object,"Label separation unsuccessfull.Exiting the separate column method of the preprocessing class")
            raise Exception()
        
        






        


