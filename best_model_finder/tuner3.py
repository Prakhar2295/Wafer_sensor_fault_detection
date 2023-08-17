import mlflow
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score,roc_auc_score
from sklearn.model_selection import GridSearchCV
from application_logging.logger import App_Logger
from mlflow.tracking import MlflowClient



class model_finder:
	"""
	      This class wil be used to find the best the model using roc_auc_score and accuracy score.

	       Written by: JSL
           Version :1.0
           Revision: None

	"""
	def __init__(self):
		self.logger_object = App_Logger()
		self.file_path = "Training_Logs/ModelTrainingLog.txt"
		self.xg = XGBClassifier(objective='binary:logistic')
		self.RF = RandomForestClassifier()
		self.client = MlflowClient()

	def get_best_params_for_xgboost(self,x_train,y_train):
		"""
		       Method name: get_best_params_for_xgboost
                Description: Get the best parameters for xgboost algorithm which gives the
                best accuracy using hyperparameter tuning.

                Output:The model with the best parameters for xgboost algorithm.
                On failure: Raises an exception


                Written by: JSL
                Version :1.0
                Revision: None


		"""
		try:
			uri = "http://127.0.0.1:5000"
			mlflow.set_tracking_uri(uri = uri)
			self.experiment_name = 'XGBOOST'
			try:
				self.exp_id = mlflow.create_experiment(name=self.experiment_name)
			except Exception as e:
				self.exp_id = mlflow.get_experiment_by_name(self.experiment_name).experiment_id


			"""try:
				experiment =self.client.create_experiment("Experiment: XGBoost")
			except:
				experiment = self.client.get_experiment_by_name("Experiment: XGBoost").experiment_id
			self.parent_run = self.client.create_run(experiment_id=experiment, tags= {"Name": "XGBOOST"})"""



			self.file_object = open(self.file_path,'a+')
			self.logger_object.log(self.file_object,"Enterd inside the get_best_params_for_xgboost method inside model_finder")
			self.file_object.close()
			#self.grid_param = {"n_estimators":[100,200,400,1000],"max_depth": [2,5,10,20,30],"learning_rate":[0.1,0.01,0.2,0.02]}

			#self.grid_xg = GridSearchCV(estimator = self.xg,param_grid= self.grid_param,verbose = 3,cv = 2)
			#self.grid_xg.fit(x_train,y_train)

			#self.file_object = open(self.file_path, 'a+')
			#self.logger_object.log(self.file_object,"Defined the grid search cv hyperparamaters for XGboost::{params}".format(params = self.grid_param))
			#self.file_object.close()

			#self.learning_rate = self.grid_xg.best_params_["learning_rate"]
			#self.n_estimators = self.grid_xg.best_params_["n_estimators"]
			#self.max_depth = self.grid_xg.best_params_["max_depth"]


			self.learning_rate = 0.01
			self.n_estimators = 10
			self.max_depth = 3


			#self.client.log_param(self.parent_run.info.run_id,"learning_rate" ,self.learning_rate)
			#self.client.log_param(self.parent_run.info.run_id, "n_estimators", self.n_estimators)
			#self.client.log_param(self.parent_run.info.run_id, "max_depth", self.max_depth)

			with mlflow.start_run(experiment_id=self.exp_id) as xg_run:
				mlflow.log_param("learning_rate",self.learning_rate)
				mlflow.log_param("n_estimators",self.n_estimators)
				mlflow.log_param("max_depth",self.max_depth)
				#mlflow.end_run()
				self.run_id_XG = xg_run.info.run_id
				print("xg_run_id:" + str(self.run_id_XG))




			#self.gamma = self.grid_xg.best_params_["gamma"]
			#self.objective = "binary:logistic"
			self.xg = XGBClassifier(n_estimators=self.n_estimators,max_depth = self.max_depth,learning_rate=self.learning_rate)

			self.xg.fit(x_train,y_train)

			return self.xg
		except Exception as e:
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Exception occurred in getting best_params_for_xgboost method ::%s"%e)
			self.file_object.close()
			raise e

	def get_best_params_for_random_forest(self,x_train,y_train):



		"""
		   Method name: get_best_params_for_random_forest
		   Description: Get the best parameters for Random Forest algorithm which gives the
		   best accuracy using hyperparameter tuning.

		   Output:The model with the best parameters for Random Forest algorithm.
		   On failure: Raises an exception


		   Written by: JSL
		   Version :1.0
		   Revision: None

	   """
		try:
			uri = "http://127.0.0.1:5000"
			mlflow.set_tracking_uri(uri=uri)

			experiment_name = 'Random Forest'
			try:
				self.exp_id1 = mlflow.create_experiment(name=experiment_name)
			except Exception as e:
				self.exp_id1 = mlflow.get_experiment_by_name(experiment_name).experiment_id

			"""
			try:
				experiment1 =self.client.create_experiment("Experiment: Random Forest")
			except:
				experiment1 = self.client.get_experiment_by_name("Experiment: Random Forest").experiment_id
			self.parent_run1 = self.client.create_run(experiment_id=experiment1, tags={"Name": "Random Forest"})"""

			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Enterd inside the get_best_params_for_random_forest method inside model_finder class")
			self.file_object.close()
			#self.grid_param_rf = {"n_estimators": [100,200,400,500],"max_depth": [2,5,10,20],"criterion": ["gini","entropy"],"min_samples_split": [2,4,6],"max_features":["sqrt","log2"],"ccp_alpha":[0.01,0.02]}
			#self.grid_rf = GridSearchCV(estimator=self.RF,param_grid = self.grid_param_rf,verbose=3,cv = 2)
			#self.grid_rf.fit(x_train,y_train)

			#self.file_object = open(self.file_path, 'a+')
			#self.logger_object.log(self.file_object,"Defined the grid search cv hyperparamaters for XGboost::{params}".format(params=self.grid_param_rf))
			#self.file_object.close()

			#self.n_estimators = self.grid_rf.best_params_["n_estimators"]
			#self.max_depth= self.grid_rf.best_params_["max_depth"]
			#self.criterion = self.grid_rf.best_params_["criterion"]
			#self.min_samples_split = self.grid_rf.best_params_["min_samples_split"]
			#self.max_features = self.grid_rf.best_params_["max_features"]
			#self.ccp_alpha = self.grid_rf.best_params_["ccp_alpha"]

			self.n_estimators = 10
			self.max_depth = 3
			self.criterion = "gini"
			self.min_samples_split = 3
			self.max_features = 4
			self.ccp_alpha = 0.01

			#self.client.log_param(self.parent_run1.info.run_id, "n_estimators", self.n_estimators)
			#self.client.log_param(self.parent_run1.info.run_id, "max_depth", self.max_depth)
			#self.client.log_param(self.parent_run1.info.run_id, "criterion", self.criterion)
			#self.client.log_param(self.parent_run1.info.run_id, "min_samples_split", self.min_samples_split)
			#self.client.log_param(self.parent_run1.info.run_id, "ccp_alpha", self.ccp_alpha)


			with mlflow.start_run(experiment_id=self.exp_id1) as rf_run:
				mlflow.log_param("n_estimators",self.n_estimators)
				mlflow.log_param("max_depth",self.max_depth)
				mlflow.log_param("criterion",self.criterion)
				mlflow.log_param("min_samples_split", self.min_samples_split)
				mlflow.log_param("max_features", self.max_features)
				mlflow.log_param("ccp_alpha", self.ccp_alpha)
				self.run_id_RF = rf_run.info.run_id
				print("rf_run_id:" + str(self.run_id_RF))
				print(type(self.run_id_RF))














			self.RF = RandomForestClassifier(n_estimators=self.n_estimators,criterion=self.criterion,max_depth=self.max_depth,min_samples_split=self.min_samples_split,max_features=self.max_features,ccp_alpha=self.ccp_alpha)
			self.RF.fit(x_train,y_train)

			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Trained the random forest model with defined Hyperparameters successfully!!")
			self.file_object.close()
			return self.RF
		except Exception as e:
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Exception occurred in getting get_best_params_for_random_forest method ::%s" % e)
			self.file_object.close()
			raise e

	def get_best_model(self,x_train,y_train,x_test,y_test):

		"""
				   Method Name : get_best_model
				   Description: Find out best model & the best roc_auc_score

				   Output: The best model name and the model object
				   On failure: Raise Exception

				   Written by: JSL
				   Version :1.0
				   Revision: None


				"""
		try:
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Enterd inside the get_best_model method inside model_finder class!!")
			self.file_object.close()
			self.xgboost = self.get_best_params_for_xgboost(x_train, y_train)
			#self.Rforest = self.get_best_params_for_random_forest(x_train, y_train)

			self.y_predict_xg = self.xgboost.predict(x_test)     ### y_pred (prediction) using XGboost
			#self.y_predict_rf = self.Rforest.predict(x_test)###y_pred (prediction) using RandomForest

			if len(y_test.unique()) ==1:         #if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
				self.score_xg = accuracy_score(y_test, self.y_predict_xg)
				with mlflow.start_run(run_id = self.run_id_XG,experiment_id=self.exp_id):
					mlflow.log_metric("accuracy_score",self.score_xg)
				#self.client.log_metric(self.parent_run.info.run_id, "Accuracy score", self.score_xg)

				#self.score_rf = accuracy_score(y_test, self.y_predict_rf)
				self.file_object = open(self.file_path, 'a+')
				self.logger_object.log(self.file_object,"The accuracy score for the xg_boost model is" + str(self.score_xg))
				self.file_object.close()
			else:
				self.score_xg = roc_auc_score(y_test, self.y_predict_xg)
				with mlflow.start_run(run_id= self.run_id_XG,experiment_id=self.exp_id):
					mlflow.log_metric("roc_auc_score",self.score_xg)


				#self.client.log_metric(self.parent_run.info.run_id, "ROC AUC score", self.score_xg)
				#self.score_rf= roc_auc_score(y_test, self.y_predict_rf)
				self.file_object = open(self.file_path, 'a+')
				self.logger_object.log(self.file_object,"The ROC_AUC_SCORE score for the xg_boost model is" + str(self.score_xg))
				self.file_object.close()

			self.Rforest = self.get_best_params_for_random_forest(x_train, y_train)
			self.y_predict_rf = self.Rforest.predict(x_test)

			if len(y_test.unique()) == 1:   #if there is only one label in y, then roc_auc_score returns error. We will use accuracy in that case
				self.score_rf = accuracy_score(y_test, self.y_predict_rf)
				with mlflow.start_run(run_id= self.run_id_RF,experiment_id=self.exp_id1):
					mlflow.log_metric("accuracy_score",self.score_rf)
				#self.client.log_metric(self.parent_run1.info.run_id, "accuracy_score", self.score_rf)
				self.file_object = open(self.file_path, 'a+')
				self.logger_object.log(self.file_object,"The accuracy score for the Random Forest model is" + str(self.score_rf))
				self.file_object.close()
			else:
				self.score_rf = roc_auc_score(y_test, self.y_predict_rf)
				with mlflow.start_run(run_id= self.run_id_RF,experiment_id=self.exp_id1):
					mlflow.log_metric("roc_auc_score",self.score_rf)
				#self.client.log_metric(self.parent_run1.info.run_id, "ROC_AUC_score", self.score_rf)
				self.file_object = open(self.file_path, 'a+')
				self.logger_object.log(self.file_object,"The ROC auc score is for the Random Forest model is" + str(self.score_rf))
				self.file_object.close()

			if (self.score_xg > self.score_rf):
				self.file_object = open(self.file_path, 'a+')
				self.logger_object.log(self.file_object,"The best model from the hyperparameter tuning and modelling chosen is  XGBoost and the score is" + str(self.score_xg))
				self.file_object.close()
				return "XGBOOST",self.xg
			else:
				self.file_object = open(self.file_path, 'a+')
				self.logger_object.log(self.file_object,"The best model from the hyperparameter tuning and modelling chosen is  Random Forest and the score is" + str(self.score_rf))
				self.file_object.close()
				return "RANDOMFOREST",self.RF

		except Exception as e:
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Exception occurred in getting get_best_model method ::%s" % e)
			self.file_object.close()
			raise e


















































