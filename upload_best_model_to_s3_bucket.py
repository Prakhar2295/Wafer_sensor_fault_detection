import subprocess
import os
from from_root import from_root  # Make sure you have this import working

class SendModelToS3Bucket:
    """
        This class will be used to upload the best model to s3 bucket for further testing,
        and futher for deployement.

    """


    def upload(self, s3_bucket_name, mlruns_direc):

        """
                Method name:upload
                Description: This method will be used to upload the best model after evaluation,
                to s3 bucket.

                On output: None
		        Failure: Raises an exception

		        Written by: JSL
		        Revision: None
		        Version: 1.0


        """


        try:
            output = subprocess.run(["aws", "s3", "sync", mlruns_direc, "s3://{}".format(s3_bucket_name)],
                                    stdout=subprocess.PIPE, encoding='utf-8')
            print("\nSaved to bucket:", s3_bucket_name)
            return f"Done Uploading: {output.stdout}"
        except Exception as e:
            raise e

    def push_model_s3_bucket(self):
        """
                Method name:push_model_s3_bucket
                Description: This method will be used to push the best model after evaluation,
                to s3 bucket.


                On output: None
		        Failure: Raises an exception

		        Written by: JSL
		        Revision: None
		        Version: 1.0

        """

        try:
            if input("Push model To s3 (Y or N):") == 'Y':
                runs = os.path.join(from_root(), 'D:/FSDS/MAchine_Learning/wafer_mlflow/artifacts/3/2a40807569ee430194397c6263a8ee46')
                print("Path to mlruns exists:", os.path.exists(runs))
                status = self.upload(s3_bucket_name='mlops-s3-001', mlruns_direc=runs)
                print(status)
        except Exception as e:
            raise e













