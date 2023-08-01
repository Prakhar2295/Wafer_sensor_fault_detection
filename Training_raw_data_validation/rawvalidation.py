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

    def __init__(self,path):
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
            path = os.path.join("Training_Raw_files_validated/","Good_Raw")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/","Bad_Raw")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/General_log.txt",'a+')
            self.logger.log(file,"Error occurred while creating directory:: %s" %ex)
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
            path = "Training_Raw_files_validated/"
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open('Training_Logs/General_log.txt','a+')
                self.logger.log(file,"Good Raw directory has been deleted successfully!!!")
        
        except OSError as s:
            file = open("Training_Logs/General_log.txt",'a+')
            self.logger.log(file,"OS error occurred in deleting good raw training directory:: " +str(s))
            raise OSError
        
    def deleteExistingBadDataTrainingFolder(self):

        """
           Method Name: deleteExistingGoodDataTrainingFolder
           Description: This method deleted the existing Bad Raw data training directory.
           Output: None
           On failure: OS Error

           Written By: JSL
           Version: 1.0
           Revisions: None

        """
        try:
            path = "Training_Raw_files_validated/Bad_Raw"
            if os.path.isdir(path):
                shutil.rmtree(path)
                file = open("Training_Logs/General_log.txt",'a+')
                self.logger.log(file,"Deleted Bad Raw Data Training Directory")
        except OSError as s:
            file = open("Training_Logs/General_log.txt",'a+')
            self.logger.log(file,"OS error during deletion of bad raw training directory:: %s" %s)
            raise OSError

    def moveBadFilesToArchiveBad(self):


        """
           Method Name: moveBadFilesToArchive
           Description: This method deletes the directory made to bad raw taining data and also moves the data to archive
           bad directory.We archive the bad raw training data to send back to the client to notify the invalid data issue.

           Output: None
           On failure: OSError

           Written By: JSL
           version: 1.0
           Revision: None    
        
        """

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = "Training_Raw_files_validated/Bad_Raw"
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = "TrainingArchiveBadData/BadData_" + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/General_log.txt",'a+')
                self.logger.log(file,"Bad Raw Data moved to Training Raw data Archive directory")
                path = "Training_Raw_files_validated/"
                if os.path.isdir(path + "Bad_Raw"):
                    shutil.rmtree(path + "Bad_Raw")
                self.logger.log(file,"Bad Raw data directory has been deleted Successfully")
                file.close()
        except OSError as r:
            file = open("Training_Logs/General_log.txt",'a+')
            self.logger.log(file,"OS error occurred during movement of bad raw data into Archive raw data:: %s" %r)
            file.close()
            raise r
        
    def raw_file_name_validation(self,regex,LengthOfDateStampInFile):


        """
             Method Name:raw_file_name_validation
             Description: This method will be used to validate the raw file names for the Training, if
             the file name is valid it will be moved to good data and if invalid move to bad directory:

             Output: Good data and Bad data
             On failure: OS error

              Written By: JSL
              Version: 1.0
              Revisions: None


        """
        ###Deleting the previously store bad data and good data directory in case last run was  unsuccessfull and files were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()

        ###creating the directories for good and bad data
        self.createDirectoryforGoodBadRawData()

        try:
            file = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(file,"Entered the raw_file_name_validation method of training_data_validation")
            file.close()
            Bad_data_path = "Training_Raw_files_validated/Bad_Raw"
            Good_data_path = "Training_Raw_files_validated/Good_Raw"
            for file in os.listdir(self.Batch_directory):
                file_path = self.Batch_directory + '/' + file
                if re.match(regex,file):
                    file_name = re.split(".csv",file)
                    file_name = re.split('_',file_name[0])
                    if len(file_name[1]) == LengthOfDateStampInFile:
                        if file not in os.listdir(Good_data_path):
                            shutil.copy(file_path,Good_data_path)
                            f = open("Training_Logs/nameValidationLog.txt", 'a+')
                            self.logger.log(f,"Copied the valid file to good_data folder %s" %file)
                            f.close()
                        else:
                            pass
                    elif file not in os.listdir(Bad_data_path):
                        shutil.copy(file_path,Bad_data_path)
                        f = open("Training_Logs/nameValidationLog.txt", 'a+')
                        self.logger.log(f, "Copied the invalid file to bad_data folder %s" % file)
                        f.close()
                    else:
                        pass
                elif file not in os.listdir(Bad_data_path):
                    shutil.copy(file_path ,Bad_data_path)
                    f = open("Training_Logs/nameValidationLog.txt", 'a+')
                    self.logger.log(f, "Copied the invalid file to bad_data folder %s" % file)
                    f.close()
                else:
                    pass
        except OSError:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occurred while moving files to good data and bad data folder %s" %OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Exception occurred while moving files to good data and bad data folder %s" %e)
            f.close()
            raise e

    def validateColumnLength(self, NumberofColumns):
        """

             Method Name: validateColumnLength
             Description: This method will be used to validate the length of columns of the training csv files.
             The invalid files will be to bad data folder and valid files will be to good data folder.
             The csv files are missing the first column name so renaming it as "wafer".

             Output: Good data and Bad data
             On failure: OS error

              Written By: JSL
              Version: 1.0
              Revisions: None

        """
        f = open("Training_Logs/columnValidationLog.txt", 'a+')
        self.logger.log(f, "Column Length Validation Started!!")
        f.close()
        try:
            Good_data_path = "Training_Raw_files_validated/Good_Raw"
            Bad_data_path = "Training_Raw_files_validated/Bad_Raw"
            for file in os.listdir(Good_data_path):
                if file.endswith(".csv"):
                    file_path = Good_data_path + '/' + file
                    df = pd.read_csv(file_path)
                    if df.shape[1] == NumberofColumns:
                        df.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                        df.to_csv(file_path, index=None, header=True)
                        f = open("Training_Logs/columnValidationLog.txt", 'a+')
                        self.logger.log(f, "The file names with valid column lengths are :: %s" % file)
                        self.logger.log(f, "The column name has been renamed Successfully" % file)
                        f.close()
                    elif file not in os.listdir(Bad_data_path):
                        shutil.move(file_path, Bad_data_path)
                    else:
                        pass
        except OSError as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error occurred while validating the columns length %s" % e)
            f.close()
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Exception occurred while validating the columns length %s" % e)
            f.close()

    def validatemissingvaluesinwholecolumn(self):
        """

               Method Name: validatemissingvaluesinwholecolumn
                Description: This method will validate if any training csv file is having whole columns values missing.
                If any such csv file is found it is send to bad raw data directory,as these files are not
                fit for processing.
                In the input given csv file the first column name is missing,this method changes it's name to "wafer".

              Output: None
              On failure: Exception

              Written By: JSL
              Version: 1.0
              Revisions: None


        """
        Good_data_path = "Training_Raw_files_validated/Good_Raw"
        Bad_data_path = "Training_Raw_files_validated/Bad_Raw"
        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing values validation started!!")
            for file in os.listdir(Good_data_path):
                file_path = Good_data_path + '/' + file
                if file.endswith(".csv"):
                    # file_path = Good_data_path + '/' + file
                    df = pd.read_csv(file_path)
                    count = 0
                    for cols in df:
                        if (df[cols].count()) == 0:
                            # print(cols)
                            count += 1
                            if file not in os.listdir(Bad_data_path):
                                shutil.move(file_path, Bad_data_path)
                                f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
                                self.logger.log(f, "Invalid files moved from good data to bad data %s" % file)
                                f.close()
                            else:
                                pass
                    if count == 0:
                        df.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                        df.to_csv(file_path, index=None, header=True)
                    f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
                    self.logger.log(f, "Unnamed column name changed successfully")
                    f.close()

        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error occurred while validating the column length. %s" % OSError)
            raise OSError
        except Exception as e:
            self.logger.log(f, "Exception occurred while validating the column length. %s" % OSError)
            f.close()
            raise e
        



        
              


            
        




















       
        
        

    









                 
        
                


















    