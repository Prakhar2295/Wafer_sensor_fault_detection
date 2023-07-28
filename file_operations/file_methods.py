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
       
    def load_nodel(self,filename):

        """
             Method Name: load_nodel
             Description: This method will load the trained model saved in a pickle file.
             Output: Load the trained model into memory
             On failure: Raises an exception

             Written By: JSL
             Version: 1.0
             Revisions: None

        """
        self.logger_object.log(self.file_object, 'Entered the load_model method of the file_operation class')
        try:
            with open(self.model_directory + filename + '/' + filename + '.sav','rb') as f:

                self.logger_object.log(self.file_object,'Model File' + filename+ 'loaded.Exited The load_model method of the file_oipration class')
                return pickle.load(f)

        except Exception as e:
            self.logger_object.log(self.file_object," Exception occurred model loading unsuccessfull.Exception meassage ::%s" %e)
            self.logger_object.log(self.file_object,"Model loading failed.Exiting the load_model method from file_operation class")
            raise Exception() 
        
           
            
            

       
       
               
             
               
        
               
                  


                   
               
         