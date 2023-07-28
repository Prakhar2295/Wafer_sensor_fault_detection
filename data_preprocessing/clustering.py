import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods

class KMeansClustering:

    """
         This class shall be used to divide the data into clusters before training.

         Written By: JSL
         Version: 1.0
         Revisions: None
    
    
    """

    def __init__(self,logger_object,file_object):
        self.logger_object = logger_object
        self.file_object = file_object


    def elbow_plot(self,data):

        """
        
              Method Name: elbow_plot
              Description: This method is used to plot the elbow plot and
              find out the optimum no.of clusters to be created in the given dataset.

              Output: A picture saved to the directory
              On failure : Raises an Exception

              Written By: JSL
              Version: 1.0
              Revisions: None

                
        """ 
        self.logger_object.log = (self.file_object,'Enter the elbow_plot method of the Kmeans clustering class')
        wcss = []

        try:
            for i in range(1,11):
                kmeans = KMeans(n_clusters = i,init = 'k-means++',random_state = 42) ###initialize the Kmeans object
                kmeans.fit(data)  ###fitting the data to the Kmeans algorithm
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,11),wcss)  ###Plotting the graph between WCSS and no. of clusters
            plt.title("The Elbow Method")
            plt.xlabel("No. of clusters")
            plt.ylabel("WCSS")
            plt.savefig("preprocessing_data/K-Means_Elbow.PNG")   ### saving the elobow plot locally

            self.kn = KneeLocator(range(1,11),wcss,curve = 'convex', direction = 'decreasing')
            self.logger_object.log(self.file_object,'The optimum no. of clusters is :' +str(self.kn.knee)+ '.Exited the elbow method inside the kmeans clustering class')
            return self.kn.knee
        except Exception as e:
            self.logger_object.log(self.file_object,"Failed to find the no. of clusters and to draw the elbow plot")
            self.logger_object.log(self.file_object,"Exception occurred while plotting the elbow plot.Exception Message:: %s" %e)
            raise Exception()

    def create_clusters(self,data,number_of_clusters):
        """
        
             Method Name: create_clusters
             Description: This method is used to create clusters and also creates a new dataframe consists of the new dataframe information.
             OutPut: A dataframe with cluster column
             On failure : Raises an exception

             Written By: JSL
             Version: 1.0
             Revisions: None
        
        
        """
        self.logger_object.log(self.file_object,"Entered inside the create_clusters method of Kmeans clustering class")
        self.data = data
        try:
            self.kmeans = KMeans(n_clusters = number_of_clusters, init = 'k-means++',random_state = 42)
            self.y_kmeans = self.kmeans.fit(self.data)   ###divideing the data into clusters

            self.file_op = file_methods.File_operation(self.logger_object,self.file_object)

            self.data["Cluster"] = self.y_kmeans   ###create a new columns for storing the cluster information of clusters
            self.logger_object.log(self.file_object,'successfully created' + str(self.kn.knee)+ 'clusters.Exited the create clusters method of Kmeans clustering class')
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Error while creating the clusters in create clusters method of Kmeans clustering class.Exception message::' +str(e))
            self.logger_object.log(self.file_object,'Exiting the create clusters method of Kmeans clustering class')
            raise Exception()

        
           


