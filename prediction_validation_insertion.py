from datetime import datetime
from prediction_raw_data_validation.prediction_raw_data_validation import prediction_data_validation
#from data_ingestion.data_loader_prediction import data_getter_prediction
from application_logging.logger import App_Logger
from prediction_data_transformation.Datatransformationprediction import DataTransformpredict
from DataTypeValidation_Insertion_Prediction.Datatypevalidationprediction2 import dboperation

class pred_validation:
    def __init__(self,path):
        self.logger_object = App_Logger()
        self.data_transform = DataTransformpredict()
        self.raw_data_validation = prediction_data_validation(path)
        self.dboperation = dboperation()
        self.file_object = open("prediction_logs/Prediction_logs.txt",'a+')
        
        
    def prediction_validation(self):
        
        try:
            
            self.logger_object.log(self.file_object,"Start of Validation on files for prediction!!")
            ###Extrating values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns=self.raw_data_validation.values_from_schema()
            ###Creatinf the regex pattern for name validation
            regex = self.raw_data_validation.regex_creation()
            ###creating the goo-bad data directory for raw prediction files
            ####validating the names of raw prediction files
            self.raw_data_validation.raw_file_name_validation(regex,LengthOfDateStampInFile)
            ##validating the colimn length of the raw prediction files
            self.raw_data_validation.validateColumnLength(NumberofColumns)
            ###Validating the null values in whole column
            self.raw_data_validation.validatemissingvaluesinwholecolumn()
            self.logger_object.log(self.file_object, "Raw File Validation completed")
            
            self.logger_object.log(self.file_object, "Starting Data Transformation!!")
            #replacing blanks in the csv file with "Null" values to insert in table
            self.data_transform.replaceMissingwithNull()
            
            self.logger_object.log(self.file_object, "Data Transformation Completed")
            
            self.logger_object.log(self.file_object,"Creating Prediction database and Tables on the basis of given given schema!!")
            
            ##create database with the given name and if present open the connection.Create the table inside database with the given column from the schema.
            self.dboperation.createtabledb("prediction",column_names)
            self.logger_object.log(self.file_object,"Creation of database done.Cration of tabel complted successfully!!")
            
            self.logger_object.log(self.file_object,"Inserting of data into table started !!!!")
            ##insert csv files into the table
            self.dboperation.insertIntoTableGoodData("prediction")
            
            self.logger_object.log(self.file_object,"Inserting data into table completed !!!!")
            
            self.logger_object.log(self.file_object,"Deleteing the good data directory !!!")
            
            self.raw_data_validation.deletedirectoryforGooddata()
            self.logger_object.log(self.file_object,"Deleted the good data directory !!!")
            
            self.logger_object.log(self.file_object,"Moving the bad data to the Archive bad directory!!!")
            ###Moving the bad data to Archive baad folder
            self.raw_data_validation.moveBadDatatoArchivebad()
            self.logger_object.log(self.file_object,"Bad data files moved to the Archive bad folder!!! Bad data folder Deleted!!!")
            
            self.logger_object.log(self.file_object,"File Validation Operation completed!!!")
            self.logger_object.log(self.file_object,"Extracting CSV files from the Database table")
            ###Export data from to CSV file
            
            self.dboperation.selectingDatafromtableintocsv("prediction")
            self.logger_object.log(self.file_object,"Expoerting the data from table to csv file Completed succesfully!! File Saved!!")

        
        except Exception as e:
            raise e    
            
path = "D:/FSDS/MAchine_Learning/wafer_sensor_fault/Prediction_Batch_files"            
p = pred_validation(path)
p.prediction_validation()
           
            
            
            
             
             
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
        
       