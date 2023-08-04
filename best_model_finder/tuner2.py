from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score,roc_auc_score
from sklearn.model_selection import GridSearchCV
from application_logging.logger import App_Logger


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
			self.file_object = open(self.file_path,'a+')
			self.logger_object.log(self.file_object,"Enterd inside the get_best_params_for_xgboost method inside model_finder")
			self.file_object.close()
			self.grid_param = {"n_estimators":[100,200,400,1000],"max_depth": [2,5,10,20,30],"learning_rate":[0.1,0.01,0.2,0.02]}

			self.grid_xg = GridSearchCV(estimator = self.xg,param_grid= self.grid_param,verbose = 3,cv = 2)
			self.grid_xg.fit(x_train,y_train)

			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Defined the grid search cv hyperparamaters for XGboost::{params}".format(params = self.grid_param))
			self.file_object.close()

			self.learning_rate = self.grid_xg.best_params_["learning_rate"]
			self.n_estimators = self.grid_xg.best_params_["n_estimators"]
			self.max_depth = self.grid_xg.best_params_["max_depth"]
			#self.gamma = self.grid_xg.best_params_["gamma"]
			#self.objective = "binary:logistic"
			self.xg = XGBClassifier(n_estimators=100,max_depth = 2,learning_rate=0.1)

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
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Enterd inside the get_best_params_for_random_forest method inside model_finder class")
			self.file_object.close()
			self.grid_param_rf = {"n_estimators": [100,200,400,500],"max_depth": [2,5,10,20],"criterion": ["gini","entropy"],"min_samples_split": [2,4,6],"max_features":["sqrt","log2"],"ccp_alpha":[0.01,0.02]}
			self.grid_rf = GridSearchCV(estimator=self.RF,param_grid = self.grid_param_rf,verbose=3,cv = 2)
			self.grid_rf.fit(x_train,y_train)

			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Defined the grid search cv hyperparamaters for XGboost::{params}".format(params=self.grid_param_rf))
			self.file_object.close()

			self.n_estimators = self.grid_rf.best_params_["n_estimators"]
			self.max_depth= self.grid_rf.best_params_["max_depth"]
			self.criterion = self.grid_rf.best_params_["criterion"]
			self.min_samples_split = self.grid_rf.best_params_["min_samples_split"]
			self.max_features = self.grid_rf.best_params_["max_features"]
			self.ccp_alpha = self.grid_rf.best_params_["ccp_alpha"]

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
			self.xg = self.get_best_params_for_xgboost(x_train, y_train)
			self.RF = self.get_best_params_for_random_forest(x_train, y_train)
			self.y_predict_xg = self.xg.predict(x_test)     ### y_pred (prediction) using XGboost
			self.y_predict_rf = self.RF.predict(x_test)     ###y_pred (prediction) using RandomForest
			if len(y_test.unique()) ==1:
				self.score_xg = accuracy_score(y_test, self.y_predict_xg)
				self.score_rf = accuracy_score(y_test, self.y_predict_rf)
			else:
				self.score_xg = roc_auc_score(y_test, self.y_predict_xg)
				self.score_rf= roc_auc_score(y_test, self.y_predict_rf)

			if self.score_xg > self.score_rf:
				return "XGBOOST",self.xg
			else:
				return "RANDOMFOREST",self.RF
		except Exception as e:
			self.file_object = open(self.file_path, 'a+')
			self.logger_object.log(self.file_object,"Exception occurred in getting get_best_model method ::%s" % e)
			self.file_object.close()
			raise e


















































