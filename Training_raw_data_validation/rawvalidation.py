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
            path = "Training_Raw_files_validated"
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
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
           bad directory.We archive the bad raw taining data to send back to the client to notify the invalid data issue.

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
        
    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):

        """
           Method Name: validationFileNameRaw
           Description: This method validates the raw data file name with the predefined schema
           and divides the raw data dile into 2 directory good raw data and bad raw data.Regex pattern
           and LengthOfDateStampInFile and LengthOfTimeStampInFile is used to validate the file name.
           
           Output : Good Raw data and Bad Raw Data
           On failure: Exception

           Written By: JSL
           version: 1.0
           Revision: None 

        """ 

        #pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        ##Delete the directory of good and bad raw data in the directory in case
        ###the last run was unsuccessfull and the folder wer not deleted
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()


        ##Create new directory
        self.createDirectoryforGoodBadRawData()
        onlyfiles = [f for f in os.listdir(self.Batch_directory)]


        try:
            file = open("Training_Logs/nameValidationLog.txt",'a+')
            for filename in onlyfiles:
                if (re.match(regex,filename)):
                    splitAtDot = re.split('.csv',filename)
                    splitAtDot = (re.split('_',splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy(self.Batch_directory + '/' + filename, "Training_RAW_files_validated/Good_Raw")
                            self.logger.log(file,"Valid File Name !! File copied to Good Raw data folder %s" %filename)

                        else:
                            shutil.copy(self.Batch_directory + '/'+ filename,"Training_RAW_files_validated/Bad_Raw")
                            self.logger.log(file,"Invalid File Name !! File copied to Bad Raw data folder %s" %filename)
                    else:
                        shutil.copy(self.Batch_directory + '/'+ filename,"Training_RAW_files_validated/Bad_Raw")
                        self.logger.log(file,"Invalid File Name !! File copied to Bad Raw data folder %s" %filename)
                else:
                    shutil.copy(self.Batch_directory + '/'+ filename,"Training_RAW_files_validated/Bad_Raw")
                    self.logger.log(file,"Invalid File Name !! File copied to Bad Raw data folder %s" %filename)
            file.close()
        except Exception as e:
            file = open('Training_Logs/nameValidationLog.txt','a+')
            self.logger.log(file,"Error occured while validating filenames:: %s" %e)
            file.close()
            raise e
        
    def validateColumnLength(self,NumberofColumns):
        """
                          Method Name: validateColumnLength
                          Description: This function validates the number of columns in the csv files.
                                       It is should be same as given in the schema file.
                                       If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                       If the column number matches, file is kept in Good Raw Data for processing.
                                      The csv file is missing the first column name, this function changes the missing name to "Wafer".
                          Output: None
                          On Failure: Exception

                           Written By: JSL
                          Version: 1.0
                          Revisions: None


                 """
        dest_path = "Training_Raw_files_validated/Bad_Raw"
        src_path =  "Training_Raw_files_validated/Good_Raw"
        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation Started!!")
            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    print("file:" + str(file))
                    shutil.move( src_path + "/" + file, dest_path)
                    print(src_path + "/" + str(file), dest_path)
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occurred while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()
        
    def validateMissingValuesInWholeColumn(self):

        """
            Method Name: validateMissingValuesInWholeColumn
            Description: This method will validate if any csv file is having whole columns values missing.
            If any such csv file is found it is send to bad raw data directory,as these files are not
            fit for processing.
            In the input given csv file the first column name is missing,this method changes it's name to "wafer".


            Output: None
            On failure: Exception

            Written By: JSL
            version: 1.0
            Revision: None

        """ 
        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Missing values validation started!!")

            for file in listdir("Training_Raw_files_validated/Good_Raw/"):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" +file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" +file,"Training_Raw_files_validated/Bad_Raw/")
                        self.logger.log(f,"Invalid file!! File move Bad Raw Data folder:: %s" %file)

                if count == 0:
                    csv.rename(columns = {"Unnamed : 0" : "wafer"},inplace = True)
                    csv.to_csv("Training_Raw_files_validated/Good_Raw/" +file, index = None,header = True)
        
        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Error occurred %s" %OSError)
            f.close()
            raise OSError

        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Error occurred %s" %e)
            f.close()
            raise e
        f.close()








        
              


            
        




















       
        
        

    









                 
        
                


















    