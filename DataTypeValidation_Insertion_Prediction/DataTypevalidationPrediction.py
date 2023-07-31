import mysql.connector as connection
import pandas as pd
import shutil
import os
import csv
from application_logging.logger import App_Logger
from datetime import datetime


class dboperation:
	"""
	     This class will be used to handle all the MYSQL database operations.

	     Written By: JSL
         Version: 1.0
         Revisions: None

	"""
	def __init__(self):
		#self.databasename = "prediction"
		self.badDatapath = "Prediction_Raw_files_validated/Bad_Raw"
		self.goodDatapath = "Prediction_Raw_files_validated/Good_Raw"
		self.logger = App_Logger()

	def databaseconnection(self,databasename):
		"""
			Method Name: databaseconnection
			Description: This Method creates the database and if database already exists then cretes the connection to the db.

			Output: Connection to the db
			On failure:Raise Connection Error

			Written by: JSL
			Version: 1.0
			Revisions: None

		"""
		try:
			log_file_path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/prediction_logs/dbconnection.txt'
			file = open(log_file_path, 'a+')
			self.logger.log(file, "Entered inside databaseconnection method inside dboperation class")
			file.close()
			conn = connection.connect(host="localhost", user="prakhar", passwd="123456", use_pure=True)
			cur = conn.cursor()
			query = "show databases"
			cur.execute(query)
			database_list = cur.fetchall()  ###This will create the list of tuples existing databases
			database_list_new = []  ###This will extract the tuples and append it to the database_list_new
			for data_base in database_list:
				database_list_new.append(data_base[0])
			#print(database_list_new)
			if databasename not in database_list_new:
				query = "create database %s" %databasename
				cur.execute(query)
				conn.close()
				file = open(log_file_path, 'a+')
				self.logger.log(file, "Database created,query executed successfully:: %s"%databasename)
				file.close()
			else:
				file = open(log_file_path, 'a+')
				conn = connection.connect(host="localhost",database = databasename,user="prakhar", passwd="123456",use_pure=True)
				self.logger.log(file, "Database already existed:: %s" %databasename)
				file.close()
			return conn	
		except ConnectionError:
			file = open(log_file_path ,'a+')
			self.logger.log(file, "Connection Error occurred while creating database")
			#conn.close()
			raise ConnectionError
		except Exception as e:
			file = open(log_file_path, 'a+')
			self.logger.log(file, "Exception occurred while creating database.Exception message::%s" %e)
			file.close()
			#conn.close()
			raise e
	def createtabledb(self,database,columnnames):
         """
              Method name:createtabledb
              Description: This method will be used to create table inside db for prediction data
              
              Output: Mysql table
              On Failure: Connection Error,Exception
         
         """
         log_file_path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/prediction_logs/createtabledb.txt'
         log_file = open(log_file_path,'a+')   
         self.logger.log(log_file,"Entered Inside the Create table db method inside the db operations class ")
         conn = self.databaseconnection(database)
         #conn = connection.connect(host="localhost", user="prakhar",database="nanotube",passwd="123456",use_pure=True)
         cur = conn.cursor()
         query = "DROP TABLE IF EXISTS Good_Raw_Data"
         cur.execute(query)
         self.logger.log(log_file,"Deleted the Existing table Good_RAW_Data")
         log_file.close()   
         try:   
             for col,type in columnnames.items():
                 if col == "Wafer":
                        #print(i,j+str(255))
                        query = "create table Good_Raw_Data (`{data}` {data1}) ".format(data =(col),data1=("varchar(255)"))
                        cur.execute(query)
                        log_file = open(log_file_path,'a+') 
                        self.logger.log(log_file,"Created the table inside the database.Query executed successfully")
                        log_file.close()
                        #print(query)      
                 else:
                    #print(i,j)
                    query = "ALTER TABLE Good_Raw_Data ADD COLUMN `{data}` {data1} ".format(data =(col),data1=(type))
                    print(query)
                    cur.execute(query)
                    log_file = open(log_file_path,'a+')
                    self.logger.log(log_file,"Altered the table in the database.%s"%database)
                    log_file.close()
                    log_file_path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/prediction_logs/dbconnection.txt'
                    file = open(log_file_path, 'a+')
                    self.logger.log(log_file,"Database connections closed successfully inside the database.%s"%database)
                    log_file.close()
                    #conn.close()
         except OSError:
             log_file_path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/prediction_logs/createtabledb.txt'
             log_file = open(log_file_path,'a+')   
             self.logger.log(log_file,"Table Creation Unsuccessfull!!.Error occurred %s" %OSError)
             #conn.close()
             raise OSError
         except Exception as e:
             #self.logger.log(log_file,"Exception occurred while creating table %s" %e)
             log_file.close()
             #conn.close()
             raise e
	     
                
                
                
                
            
            
                

            














































