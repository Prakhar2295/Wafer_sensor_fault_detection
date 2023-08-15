#import cassandra
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd


class training_data_from_db:
    
    """
    
                This class will be used to ingest the raw training data from the client side database source.
                
                Written by: JSL
                Version: 1.0
                Revisions: None
                
    
    """
    
    def __init__(self):
        pass
    
    
    def cluster_connection(self):
        
        """
                Method name: cluster_connection
                Description: This method wil be used to check the cluster authorization and establish the cluster connection.
                
                Return: None
                Failure: Exception
                
                Written by: JSL
                Version: 1.0
                Revisions: None
        
        """
        try:
            cloud_config= {'secure_connect_bundle': 'D:\FSDS\MAchine_Learning\secure-connect-training-data.zip'}
            auth_provider = PlainTextAuthProvider('aPKRaxqWpaJSEPPBiZXLRMws', 'IUUZrXnxLlsMTf-KuurdS5MIo1GteEIDUGwn0dALZKwTGnTDaJLBe,sClyZ.cym6nZ5ECjYY238_,+-Slyv+gZiuf.8JH6IlLPXuDDbIRZ3_zAgoGI67+WLrM0G3Z-Q4')
            self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = self.cluster.connect()
            row = self.session.execute("select release_version from system.local").one()
            if row:
                return True
            else:
                return("connection failed")
        except Exception as e:
            raise e
        
    def create_database(self,keyspace_name):         ###where keyspace name is database name ###
        """
              Method Name: create_database
              Description: This method will be used to create the database for the new training data
              received from the client side.
              
              Return: None
              Failure: Exception
            
              Written by: JSL
              Version: 1.0
              Revisions: None
        
        """
        try:
            result = self.session.execute("SELECT * FROM system_schema.keyspaces;")
            keyspace_names = [row.keyspace_name for row in result]
            if keyspace_name in keyspace_names:
                pass
            else:
                query = f"CREATE KEYSPACE {keyspace_name} WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 4};"
                row = self.session.execute(query)
            return keyspace_name
            
        except Exception as e:
            raise e

                
    def data_ingestion(self,keyspace_name,table_name):
        """
        
                Method name: data_ingestion
                Description: This method will used to ingest the data from the client db to to the local sytem.
                
                Return: Data in the CSV format
                Failure: Exception
                
                Written by: JSL
                Version: 1.0
                Revisions: None
        
        """
        connection= self.cluster_connection()
        
        keyspace_name = self.create_database(keyspace_name)
        #table_name = 'training_data'
        
        now = datetime.now()
        date = now.date()
        time = now.strftime('%H%M%S')
        # Retrieve the table metadata
        if connection:
            metadata = self.cluster.metadata.keyspaces[keyspace_name].tables
            if table_name in metadata:
                query = f"SELECT * FROM wafer_sensor.{table_name};"
                df = pd.DataFrame(list(self.session.execute(query)))
                df.to_csv("wafer_"  + str(date) +"_"+ str(time) + ".csv",index = None)
                query = f"DROP TABLE IF EXISTS wafer_sensor.{table_name}"
                row = self.session.execute(query)
            else:
                print("Table not found")
        else:
            print("connection not established")