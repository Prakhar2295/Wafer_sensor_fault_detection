import mysql.connector as connection
import os
from DataTypeValidation_Insertion_Training.Datatypevalidation_mysql_training2 import dboperation

class find_best_nodel:

	"""
			This class will be used to set up the database connection using SQL,
			where all the models params & metrics are being stored.


	"""
	def __init__(self):
		self.db = dboperation()




	def create_model_evaluation_table_in_db(self,databasename):
		"""

				Method name:create_model_evaluation_table_in_db
				Description: This method will be used to find the params & metrics stored
				in the db.Thi will fetch run_id with the best metrics and their respective params.

				Output: Create table in DB
				On failure:Raise Connection Error

				Written by: JSL
				Version: 1.0
				Revisions: None


		"""
		try:
			conn = self.db.databaseconnection(databasename)
			cur = conn.cursor()
			query = "DROP TABLE IF EXISTS model_evaluation"
			cur.execute(query)
			query = "CREATE TABLE model_evaluation AS SELECT p.run_uuid AS param_run_uuid, p.key AS param_key,p.value AS param_value, m.run_uuid AS metric_run_uuid,m.key AS metric_key,m.value AS metric_value FROM metrics m JOIN params p ON p.run_uuid = m.run_uuid;"
			cur.execute(query)
		except Exception as e:
			raise e

	def get_artifacts_run_id(self):
		"""
				Method name:get_artifacts_run_id
				Description: This method will be used to get the artifacts run id stored in the local folder.
				This will fetch run_id and their model name.

				Output: Artifacts run id list
				On failure:Raise Connection Error

				Written by: JSL
				Version: 1.0
				Revisions: None

		"""
		try:
			path =  "artifacts/3"
			model_run_id_list = []
			for model_run_id in os.listdir(path):
				model_run_id_list.append(model_run_id)
			#print(model_run_id_list)
			return model_run_id_list
		except Exception as e:
			raise e

	def find_model_run_ids_from_db(self,databasename):
		"""
					Method name: find_model_run_ids_from_db
					Description: This method will be used to find the best model using the model evaluation table in the
					DB.
					Output: Create table in DB
					On failure:Raise Connection Error

					Written by: JSL
					Version: 1.0
					Revisions: None

		"""
		try:
			conn = self.db.databaseconnection(databasename)
			cur = conn.cursor()
			model_run_id_list = self.get_artifacts_run_id()
			self.result = []
			for run_id in model_run_id_list:
				query = f"SELECT param_run_uuid,metric_key,metric_value FROM model_evaluation WHERE param_run_uuid IN ('{run_id}');"
				cur.execute(query)
				#print(query)
				self.result.append(cur.fetchall())
			#print(self.result)
			return self.result
		except Exception as e:
			raise e

	def get_best_model_run_id_cluster_no(self,databasename):
		"""
					Method name: get_best_model_run_id_cluster_no
					Description: This method will be used to get the best model name and run_id using the model evaluation table in the
					DB and artifacts present in the local folder which will create a dictionary which will be logged ,so the results can be easily evaluated.

					Output: Creates dictionary with run id,cluster no,model name
					On failure:Raise Connection Error

					Written by: JSL
					Version: 1.0
					Revisions: None

		"""
		try:
			self.result = self.find_model_run_ids_from_db(databasename)
			model_run_id_metrics_dict = {}
			for i in self.result:
				#print(i[0][2])
				model_run_id_metrics_dict[i[0][0]] = {f"{i[0][1]}": f"{i[0][2]}"}
			path = "artifacts/3"
			model_name = {}
			for run_id in os.listdir(path):
				for model in os.listdir(path + "/" + run_id + "/" + "artifacts/"):
					model_name[run_id] = model
			print(model_name)
			run_model_name_cluster_no = {}
			for run_id, model_name in model_name.items():
				if run_id in model_run_id_metrics_dict.keys():
					score = model_run_id_metrics_dict[run_id]
					run_model_name_cluster_no[run_id] = {'score': score, 'cluster_no': model_name}
			print(run_model_name_cluster_no)
			return run_model_name_cluster_no
		except Exception as e:
			raise e

a = find_best_nodel()
#a.create_model_evaluation_table_in_db("wafer2")
a.get_artifacts_run_id()
a.find_model_run_ids_from_db("wafer2")
a.get_best_model_run_id_cluster_no("wafer2")
print("done")




























