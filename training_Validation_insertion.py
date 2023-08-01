from datetime import datetime
from Training_raw_data_validation.rawvalidation import Raw_Data_Validation
from DataTypeValidation_Insertion_Training.Datatypevalidation_mysql_training2 import dboperation
from DataTransform_Training.DataTransformation import dataTransform
from application_logging import logger


class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_Validation(path)
        self.dataTransform = dataTransform()
        self.dboperation = dboperation()
        self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()


    def train_validation(self):
        try:
            self.log_writer.log(self.file_object, "Start of validation on files")
            ###Extracting the values from the prediction schema
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,NumberofColumns = self.raw_data.valuesFromSchema()
            ##Getting the regex defined to validate filename
            regex = self.raw_data.manualregexcreation()
            ###validating filename of raw data files
            self.raw_data.raw_file_name_validation(regex,LengthOfDateStampInFile)
            ##validate column length in the file
            self.raw_data.validateColumnLength(NumberofColumns)
            ##validating if any column has all the values missing
            self.raw_data.validatemissingvaluesinwholecolumn()
            self.log_writer.log(self.file_object,"Raw Data File Validation completed!!")

            self.log_writer.log(self.file_object,"Starting Data Transformation")

            self.dataTransform.replaceMissingWithNull()

            self.log_writer.log(self.file_object,"Data Transformation Completed !!")

            self.log_writer.log(self.file_object,"Creating Training_Database and tables on the basis of given schema!!")
            # create database with given name, if present open the connection! Create table with columns given in schema

            self.dboperation.createtabledb("training",column_names)
            self.log_writer.log(self.file_object,"Table creation Completed!!")

            self.log_writer.log(self.file_object,"Insertion of Data into Table started!!!!")
            ## inserting csv files in the table

            self.dboperation.insertIntoTableGoodData("training")
            self.log_writer.log(self.file_object,"Inserting in table completed !!!")
            self.log_writer.log(self.file_object,"Deleting Good Data Folder !!!")
           ### Deleting the good data training folder
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_object,"Deleted the good data training folder")
            self.log_writer.log(self.file_object,"Moving Bad Data files to archive folder and deleting the bad data folder")
           ## Moveing the bada raw raw data files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object,"Moved the bad data to archive !! Deleted the good data training folder!!")
            self.log_writer.log(self.file_object,"Validation operation completed !!")
            self.log_writer.log(self.file_object,"Extracting csv files from table")
            ## Export data from table to csv file
            self.dboperation.selectingDatafromtableintocsv("training")
            self.log_writer.log(self.file_object, "Export data from database table to csv file completed successfully!!")
            self.file_object.close()

        except Exception as e:
            raise e
        
path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/Training_Batch_Files'
f = train_validation(path)
f.train_validation()
print("done")

















    


