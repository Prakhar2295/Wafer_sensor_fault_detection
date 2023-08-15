import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime

class mongodb_logger:
    
    """
    
                This class will be used to log the messages in the mongo db database
                and all the exceptions inside the project.
                
                 
    """
    def __init__(self,client_url):
        self.client_url = client_url
    
    def mongodb_connection(self):
        
        """

                Method name: mongodb_connection
                Description: This method will be used check the the connection with the mongodb client
                if the mongodb client server is available or not.
                
                Output: None
                On failure: Exception
                
                
                Written by: JSL
                Version: 1.0
                Revisions: None 
        
        
        """
        try:
            self.default_connection_url = self.client_url
            self.client = pymongo.MongoClient(self.default_connection_url)
            if len(self.client.list_database_names()) >= 1:
                return True
            else:
                print("client server not available")
        except ConnectionFailure:
            raise ConnectionFailure
        
        
    def create_database(self,database_name):
        
        """
        
                Method name: create_database
                Description: This method will be used to create a database inside the mongodb server.
                
                Output: None
                On failure: Exception
                
                
                Written by: JSL
                Version: 1.0
                Revisions: None 
        
        
        
        """
        try:
            self.connection = self.mongodb_connection()
            if self.connection:
                self.database = self.client[database_name]
                return self.database
            else:
                print("client server not available")
        except Exception as e:
            raise e
        
    def create_collection(self, database_name,collection):
        
        """
        
                    Method name: create_collection
                    Description: This method will be used to create the collections inside the mongodb server database.
                
                    Output: None
                    On failure: Exception
                    
                    
                    Written by: JSL
                    Version: 1.0
                    Revisions: None 
                        
        
        """
        try:
            self.database = self.create_database(database_name)
            self.collection = self.database[collection]
            return self.collection
        except Exception as e:
            raise e
        
    def insert_records_into_collection(self,database_name,collection,message):
        
        """
        
                Method name: insert_records_into_collection
                Description: This method will be used to insert records inside the collections of the database of mongodb server.
                
                Output: None
                On failure: Exception
                
                
                Written by: JSL
                Version: 1.0
                Revisions: None
        
        
        """
        
        try:
            self.database = self.create_database(database_name)
            self.collection = self.create_collection(database_name,collection)
            now = datetime.now()
            date = now.date()
            time = now.strftime('%H:%M:%S')
            record = {
                "log_updated_date": str(date),
                "log_updated_time": str(time),
                "message": message,
                "project": "wafer_sensor",
                "updated date and time": str(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                    
            }
            return self.collection.insert_one(record)
        except Exception as e:
            raise e
            
            
            
        
                    
        
            
            
                
                
        
        
        
      
        
        
      
        
       
        
        
        



