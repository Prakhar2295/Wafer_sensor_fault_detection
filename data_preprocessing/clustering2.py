import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods
from application_logging.logger import App_Logger
import mlflow

class clustering:

	"""
	     This class shall be used to divide the datasets into clusters before training.

         Written by: JSL
         Version: 1.0
         Revision: None

	"""

	def __init__(self):
		self.logger_object = App_Logger()
		self.file_path = "Training_logs/Main_Training_log.txt"


	def elbow_plot(self,data):
		"""
		      Method Name: elbow_plot
		      Description: This class will be used to find the numbers of clusters to be
		      on which the whole dataset will be divided.

		      Output: No. of clusters to be created
		      On failure: Raises an Exception

			  Written by: JSL
	          Version: 1.0
	          Revision: None


		"""
		try:
			self.file_object = open(self.file_path,'a+')
			self.logger_object.log(self.file_object,"Entered inside the elbow plot method inside the clustering class")
			self.file_object.close()
			#uri = "http://127.0.0.1:5000"
			#mlflow.set_tracking_uri(uri=uri)

			wcss = []
			for i in range(1,15):
				kmean = KMeans(n_clusters = i,init = 'k-means++',random_state = 150)
				kmean.fit(data)
				wcss.append(kmean.inertia_)
			plt.plot(range(1,15),wcss)
			plt.title("Elbow Plot")
			plt.xlabel("No.of clusters")
			plt.ylabel("WCSS")
			plt.savefig("preprocessing_data/K-Means_Elbow1.PNG")
			self.kn = KneeLocator(range(1 , 15),wcss,curve = 'convex',direction = 'decreasing')
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Elbow plot hase been created successfully!!Number of clusters will be ::%s"%self.kn)
			self.file_object.close()
			return self.kn.knee
		except Exception as e:
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Plotting elbow plot failed.Exception occurred::%s"%e)
			self.file_object.close()
			raise e

	def create_clusters(self,data,Noofclusters):
		"""

		      Method name: create_clusters
		      Description: This method will be used to create clusters on the given dataset.

		      Output: No. of clusters to be created
		      On failure: Raises an Exception

			  Written by: JSL
	          Version: 1.0
	          Revision: None


		"""
		try:
			uri = "http://127.0.0.1:5000"
			mlflow.set_tracking_uri(uri=uri)
			self.experiment_name = 'K Means Clustering'
			try:
				self.exp_id = mlflow.create_experiment(name=self.experiment_name)
			except Exception as e:
				self.exp_id = mlflow.get_experiment_by_name(self.experiment_name).experiment_id

			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Entered insdde the create cluster inside method inside the clustering class")
			self.file_object.close()
			self.data = data


			with mlflow.start_run(experiment_id=self.exp_id):
				self.kmeans = KMeans(n_clusters=Noofclusters, init='k-means++', random_state=150)
				mlflow.log_param("no_of_clusters",Noofclusters)
				self.kmeans_pred = self.kmeans.fit_predict(self.data)
				mlflow.sklearn.log_model(self.kmeans,"KMeans")
				mlflow.end_run()

			###Dividing the dataset into the clusters
			self.data["clusters"] =  self.kmeans_pred    ##creating a new column on the dataset
			self.file_object = open(self.file_path, 'a+')
			self.file_op = file_methods.File_operation(self.file_object,self.logger_object)
			self.save_model = self.file_op.save_model(self.kmeans,"KMeans")
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Clusters created succesfully.New dataframe created with clusters!!")
			self.file_object.close()
			return self.data
		except Exception as e:
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Exception occurred.Creating clusters unsucessfull %s"%e)
			self.file_object.close()
			raise e







