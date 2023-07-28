import os
import pickle
import shutil

class File_operation:

    """
            This Class Shall be used to save the model to a file after training and
            load the saved model for prediction.

            Written By: JSL
            Version: 1.0
            Revisions: None

    
    
    """
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory = "models/"

    def save_model(self,model,filename):

                         
       """
               Method Name: save_model
               Description: Save the model file to specific directory
               Outcome: None
               On failure: Raises an Exception


                Written By: JSL
                Version: 1.0
                Revisions: None       

        
        """
       self.logger_object.log(self.file_object,"Entered the save model method inside the file_operation class")
       try:
           path = os.path.join(self.model_directory,filename)  ###Create separate directory for each cluster 
           if os.oath.isdir(path):   ###remove previously existing directory for each clusters
               shutil.rmtree(self.model_directory)
               os.makedirs(path)
           else:
               os.makedirs(path)
           with open(path +'/' + filename + '.sav','wb') as f:
               pickle.dump(model,f)
           self.logger_object.log(self.file_object,'Model File' +filename +'saved.Exited the saved_model method of the file operation class')

           return "success"
       except Exception as e:
           self.logger_object.log(self.file_object,"Exiting the saved_model method of the file operation class")
           self.logger_object.log(self.file_object,"Exception occurred while saveing the model.Exception message:: %s"%e)
           raise Exception()
       
       
               
             
               
        
               
                  


                   
               
         