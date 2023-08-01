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
		#self.databasename = "Training"
		self.badDatapath = "Training_Raw_files_validated/Bad_Raw"
		self.goodDatapath = "Training_Raw_files_validated/Good_Raw"
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
			log_file_path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/Training_Logs/dbconnection.txt'
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

	def createtabledb(self, database, columnnames):
		"""
			 Method name:createtabledb
			 Description: This method will be used to create table inside db for training data

			 Output: Mysql table
			 On Failure: Connection Error,Exception

			 Written by: JSL
			 Version: 1.0
			 Revisions: None

		"""
		log_file_path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/Training_Logs/createtabledb.txt'
		log_file = open(log_file_path, 'a+')
		self.logger.log(log_file, "Entered Inside the Create table db method inside the db operations class ")
		conn = self.databaseconnection(database)
		# conn = connection.connect(host="localhost", user="prakhar",database="nanotube",passwd="123456",use_pure=True)
		cur = conn.cursor()
		query = "DROP TABLE IF EXISTS Good_Raw_Data"
		cur.execute(query)
		self.logger.log(log_file, "Deleted the Existing table Good_RAW_Data")
		log_file.close()
		try:
			for col, type in columnnames.items():
				if col == "Wafer":
					# print(i,j+str(255))
					query = "create table Good_Raw_Data (`{data}` {data1}) ".format(data=(col),data1=("varchar(255)"))
					cur.execute(query)
					log_file = open(log_file_path, 'a+')
					self.logger.log(log_file, "Created the table inside the database.Query executed successfully!!")
					log_file.close()
				# print(query)
				else:
					# print(i,j)
					query = "ALTER TABLE Good_Raw_Data ADD COLUMN `{data}` {data1} ".format(data=(col),data1=(type))
					print(query)
					cur.execute(query)
					log_file = open(log_file_path, 'a+')
					self.logger.log(log_file, "Altered the table in the database.%s" % database)
					log_file.close()
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file, "Table created successfully completed inside the database!!%s" % database)
			log_file.close()
		except OSError:
			log_file_path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/Training_Logs/createtabledb.txt'
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file, "Table Creation Unsuccessfull!!.Error occurred %s" % OSError)
			# conn.close()
			raise OSError
		except Exception as e:
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file, "Exception occurred while creating table %s" % e)
			log_file.close()
			# conn.close()
			raise e

	def insertIntoTableGoodData(self, database):
		"""
			Method Name: insertIntoTableGoodData
			Description: This method will be used to the insert the Good Training data into the
			database table.

			Output: None
			On Failure: Connection Error,Raises an Exception

			Written by: JSL
			Version: 1.0
			Revisions: None

		"""
		try:
			log_file_path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/Training_Logs/TableDatainsertion.txt'
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file,"Entered inside the insertintoTableGooddata method inside the db operation class")
			log_file.close()
			conn = self.databaseconnection(database)
			cur = conn.cursor()
			##path = "C:/Users/prath/Desktop/PYTHON/Training_Raw_files_validated/Bad_Raw"
			for file in os.listdir(self.goodDatapath):
				file_path = self.goodDatapath + "/" + file
				# print(file_path)
				with open(file_path, mode='r') as file:
					next(file)  ###skipping the first row of the csv file with the column names
					csvFile = csv.reader(file)
					for lines in csvFile:  ###Iterating though the csv file
						#print(len(lines))
						p = ','.join(lines)
						cur.execute("INSERT INTO Good_Raw_Data values ({data})".format(data=(p)))
			conn.commit()
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file , "Inserted data into the table successfully!!")
			log_file.close()
			#conn.close()
		except ConnectionError as e:
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file, "Error Occurred.Insertion of data inside table unsuccesfull!!.%s" % e)
			log_file.close()
			#conn.close()
			raise e
		except Exception as e:
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file, "Exception Occurred.Insertion of data inside table unsuccesfull!!.Exception message::%s" % e)
			log_file.close()
			#conn.close()
			raise e

	def selectingDatafromtableintocsv(self,database):

		"""
		        Method name: selectingDatafromtableintocsv
		        Description: This method will be used to convert the data from sql table into a dataframe.
		        This dataframe will be saved to the csv file.

		        On output: A csv file
		        Failure: OS Error, Exception

		        Written by: JSL
		        Version: 1.0
		        Revisions: None



		"""
		log_file_path = "D:/FSDS/MAchine_Learning/wafer_sensor_fault/Training_Logs/DataFromTabletoCSV.txt"
		log_file = open(log_file_path, 'a+')
		self.logger.log(log_file, "Entered the selectingDatafromtableintocsv inside db operation class ")
		log_file.close()
		path = "D:/FSDS/MAchine_Learning/wafer_sensor_fault/TrainingFileFromDB"
		file_path = "D:/FSDS/MAchine_Learning/wafer_sensor_fault/TrainingFileFromDB" + "/" + "InputFile.csv"
		try:
			conn = connection.connect(host="localhost", user="prakhar", database=database, passwd="123456",use_pure=True)
			cur = conn.cursor()
			if not os.path.isdir(path):
				os.makedirs(path)
				log_file = open(log_file_path, 'a+')
				self.logger.log(log_file, "Directory Created Succesfully !! %s " % path)
				log_file.close()

			df = pd.read_sql("select * from Good_Raw_Data", conn)
			df.to_csv(file_path)
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file, "Input File csv created successfully from table%s" % file_path)
			log_file.close()
			conn.close()
			log_file_path = 'D:/FSDS/MAchine_Learning/wafer_sensor_fault/Training_Logs/dbconnection.txt'
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file, "Database connection closed succesfully::{database}".format(database = database))
			log_file.close()
		except OSError as e:
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file,"Input file csv creation unsuccessfull.Error occurred while creating csv file from mysql table %s " % e)
			log_file.close()
			raise e
		except Exception as e:
			log_file = open(log_file_path, 'a+')
			self.logger.log(log_file, "Error occurred while creating csv file>exception message::%s" % e)
			log_file.close()
			raise e


    


        
	 
	 
	     
                
                
                
                
            
            
                

            














































