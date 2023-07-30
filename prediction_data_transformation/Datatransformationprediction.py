import pandas as pd
from application_logging.logger import App_Logger
import os

class DataTransformpredict:

	"""
	     This class shall be used to Transform the Good raw prediction data before loading into the training base.

	     Written By: JSL
         Version: 1.0
         Revision: None

	"""

	def __init__(self):
		self.gooddatapath = "Raw_prediction_data/Good_data"
		self.logger = App_Logger()

	def replaceMissingwithNull(self):

		"""
			Method Name: replaceMissingwithNull
			Description: This method will be used to replace the missing values in the column with the NULL
			before loading prediction db.
			values.
			Output:None
			On failure: Exception

			Written By: JSL
			Version: 1.0
			Revisions: None

	   """
		try:
			f = open("prediction_logs/Datatransformationprediction.txt", 'a+')
			self.logger.log(f, "Entered inside the replaceMissingwithNull method inside data transform prediction class")
			for file in os.listdir(self.gooddatapath):
				file_path = self.gooddatapath + '/' + file
				df = pd.read_csv(file_path)
				df.fillna("NULL", inplace=True)
				df['Wafer'] = df['Wafer'].str[6:]
				df.to_csv(file_path, index=None, header=True)
			self.logger.log(f, "Data Transformation completed successfully")
			f.close()
		except Exception as e:
			f = open("prediction_logs/Datatransformationprediction.txt", 'a+')
			self.logger.log(f, "Exception occurred while performing data transformation %s" %e)
			f.close()
			raise e




























