from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

class Model_finder:
    
    """
           This class shall be used to find the model with best accuracy and AUC score.

           Written by: JSL
           Version :1.0
           Revision: None
 
    """
    def __init__(self, file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.clf = RandomForestClassifier()
        self.xgb = XGBClassifier(objective = "binary:logistic")

    def get_best_param_for_random_forest(self,train_x,train_y):


        """
                Method name: get_best_param_for_random_forest
                Description: Get the best parameters for Random Forest algorithm which gives the 
                best accuracy using hyperparameter tuning.

                Output:The model with the best parameters for Random Forest algorithm.
                On failure: Raises an exception


                Written by: JSL
                Version :1.0
                Revision: None
        
        """

        self.logger_object.log(self.file_object,"Entered inside the get_best_param_for_random_forest method inside the model_finder class")
        try:
            ####initiaizing the different combination of parameters
            self.param_grid= {"n_estimators":[10,50,100,150], "criterion":["gini","entropy"],
                              "max_depth": range(2,4,1),"max_features": ['auto','log2']}

            ####Creating an object for the grid search cv class
            self.grid = GridSearchCV(estimator = self.clf,param_grid = self.param_grid, cv = 5,verbose = 3)
            ###find the best parameters
            self.grid.fit(train_x,train_y)

            ##Extrating the best parameters
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.max_features = self.grid.best_params_["max_features"]
            self.n_estimators =  self.grid.best_params_['n_estimators']

            self.clf = RandomForestClassifier(n_estimators=self.n_estimators,criterion=self.criterion,
                                              max_depth = self.max_depth,max_features=self.max_features)
            

            self.clf.fit(train_x,train_y)
            self.logger_object.log(self.file_object,"Random Forest Best params::"  +str(self.grid.best_params_)+ 'Exited the get_best_param_for_random_forest method inside model_finder class')
            
            return self.clf
        except Exception as e:
            self.logger_object(self.file_object,"Exception occurred while finding the best params for Random Forest. Exception message:" +str(e))
            self.logger_object(self.file_object,"Failed to find the best params for Random Forest. Exiting this method")
            raise Exception()

    def get_best_params_for_xgboost(self,train_x,train_y):


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

        self.logger_object.log(self.file_object,"Entered inside the get_best_params_for_xgboost method inside the model_finder class")
        try:
            #self.xgb = XGBClassifier(objective = "binary:logistic")
            self.param_grid_xgboost = {"learning_rate": [0.5,0.1,0.011,0.01],
                                                     "max_depth": [3,5,10,20],
                                                     'n_estimators': [10,50,100,200]}
            
            ##Creating m
            self.grid = GridSearchCV(estimator = self.xgb,param_grid = self.param_grid_xgboost,cv = 5,verbose = 3)
            self.grid.fit(train_x,train_y)

            self.learning_rate = self.grid_best_params_["learning_rate"]
            self.max_depth = self.grid_best_param_["max_depth"]
            self.n_estimators = self.grid_best_pramas_["n_estimators"]

            self.xgb = XGBClassifier(learning_rate=self.learning_rate,max_depth = self.max_depth,n_estimators=self.n_estimators )

            self.xgb.fit(train_x,train_y)


            self.logger_object.log(self.file_object,"XGboost Best params::"  +str(self.grid.best_params_)+ 'Exited the get_best_param_for_random_forest method inside model_finder class')
            
            return self.xgb
        except Exception as e:
            self.logger_object(self.file_object,"Exception occurred while finding the best params for Random Forest. Exception message:" +str(e))
            self.logger_object(self.file_object,"Failed to find the best params for Random Forest. Exiting this method")
            raise Exception()

            
 






