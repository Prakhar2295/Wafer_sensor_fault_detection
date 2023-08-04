##Doing the necessary imports

from data_ingestion.data_loader import Data_Getter
from data_preprocessing.data_preprocessing import Preprocessing
from data_preprocessing.clustering2 import clustering
from application_logging.logger import App_Logger
from best_model_finder.tuner2 import model_finder
from sklearn.model_selection import train_test_split
from file_operations.file_methods import File_operation

"""

This is the entry point for Training the Machine Learning Model.

Written By: JSL
Version: 1.0
Revision: None


"""

class train_model:

	def __init__(self):
		self.file_path = "Training_Logs/ModelTrainingLog.txt"
		self.logger_object = App_Logger()


	def model_training(self):
		try:
			self.file_object = open(self.file_path,'a+')
			self.logger_object.log(self.file_object,"Entered the model_training method of the train_model class!! Mpdel training started!!")
			self.file_op = File_operation(self.file_object, self.logger_object)
			self.file_op.move_old_models_to_archive()
			print("Moved old models to archive")
			self.data_op = Data_Getter(self.file_object,self.logger_object)
			self.data = self.data_op.get_data()
			self.logger_object.log(self.file_object,"Preprocessing of data started!! Removal of column started!!")
			self.preprocess = Preprocessing()
			self.df_new = self.preprocess.remove_columns(self.data,["Unnamed: 0","Wafer"])
			self.logger_object.log(self.file_object, "Removal of columns done successfully.Exited the remove column methods from preprocessor class")

			###separating the columns into features and labels
			X,Y = self.preprocess.separate_label_features(self.df_new,"Output")
			self.logger_object.log(self.file_object,"Separation of clolumns done successfully.Exited the separting the columns methods from preprocessor class")

			is_null_present = self.preprocess.is_null_present(X)   ####checking if the values are present of not in the dataset
			self.logger_object.log(self.file_object,"Checking of null values inide colum done successfully.Null  columns methods from preprocessor class")

			if (is_null_present):
				self.X_new = self.preprocess.missing_value_imputation(X)

			self.logger_object.log(self.file_object,"Imputation of null values done sucecessfully inide colum done successfully. is_null_present inside columns methods from preprocessor class")
			col_drop_list = self.preprocess.cols_with_zero_std_deviation(self.X_new)
			self.logger_object.log(self.file_object,"Succesfully created a list of columns with zero stad deviation")
			self.X1 = self.preprocess.remove_columns(self.X_new,col_drop_list)

			self.logger_object.log(self.file_object,"Column with zero std deviation dropped sucecessfully inside removal columns method, from preprocessor class")
			self.logger_object.log(self.file_object,"Preprocessing of columns completed successfully!!")

			cluster = clustering()
			no_of_clusters = cluster.elbow_plot(self.X1)
			self.logger_object.log(self.file_object, "Plotted the elbow plot done Successfully!!.Found No. of clusters to be created succesfully")

			cluster_data = cluster.create_clusters(self.X1,no_of_clusters)

			self.logger_object.log(self.file_object,"Created Clusters Successfully!!")

			model = model_finder()
			####Adding the new column to the cluster data dataframe of labels
			cluster_data["labels"] = Y

			list_of_cluster = cluster_data["clusters"].unique()

			for i in list_of_cluster:
					cluster_data1 = cluster_data[cluster_data["clusters"] == i]

					cluster_features = cluster_data1.drop(labels=["labels","clusters"],axis = 1)
					cluster_labels = cluster_data1["labels"]
					x_train,x_test,y_train,y_test = train_test_split(cluster_features,cluster_labels,test_size=0.33,random_state=90)
					best_model_name,best_model = model.get_best_model(x_train,y_train, x_test, y_test)
					self.file_object = open(self.file_path,'a+')
					self.file_op = File_operation(self.file_object,self.logger_object)
					save_model = self.file_op.save_model(best_model,best_model_name + str(i))

					self.logger_object.log(self.file_object,"Model saved Successfully!!"+str(best_model_name)+str(i))

			self.file_object.close()
		except Exception as e:
			raise e


p = train_model()
f = p.model_training()
print("done")

































