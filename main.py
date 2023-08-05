from flask import Flask, request, Response
from flask_cors import CORS, cross_origin
from wsgiref import simple_server
from trainingmodel2 import train_model
from training_Validation_insertion import train_validation
from prediction_validation_insertion import pred_validation
import flask_monitoringdashboard as dashboard
from predictFromModel import prediction
import json
import os

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')



app = Flask(__name__)

dashboard.bind(app)
CORS(app)





print("flask app running")

@app.route('/train', methods=['POST','GET'])
def train_route_client():
    try:
        if request.method == 'POST':
            folderpath = "Training_Batch_Files"
            if folderpath is not None:
                path = folderpath

                # Uncomment the following lines to use the train_validation and train_model classes
                train_valid = train_validation(path)
                train_valid.train_validation()

                model_train = train_model()
                model_train.model_training()

                return Response("Training Successful!!")
        else:
            return Response("Training UNSuccessful!!")

    except KeyError as e:
        return Response("Error occurred %s" % e)
    except ValueError as e:
        return Response("Error occurred:: %s" % e)
    except Exception as e:
        return Response("Error occurred:: %s" % e)



@app.route('/predict',methods = ['POST','GET'])
def predict_route_client():
    try:
        if request.form is not None:
            path = request.form["filepath"]

            pred_valid = pred_validation(path)
            pred_valid.prediction_validation()

            pred = prediction(path)
            path,json_predictions = pred.predictionfrommodel()

            return Response("Prediction File created at !!!"  +str(path) +'and few of the predictions are '+str(json.loads(json_predictions)))
        elif request.json is not None:
            path = request.json["filepath"]

            pred_valid = pred_validation(path)
            pred_valid.prediction_validation()

            pred = prediction(path)
            path, json_predictions = pred.predictionfrommodel()
            return Response("Prediction File created at !!!" + str(path) + 'and few of the predictions are ' + str(
                json.loads(json_predictions)))
        else:
            print("Nothing Matched!!")
    except ValueError:
        return Response("Error Occurred! %s" % ValueError)
    except KeyError:
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" % e)

port = int(os.getenv("PORT",5000))
if __name__ == "__main__":
    host = '0.0.0.0'
    # port = 5000
    httpd = simple_server.make_server(host, port, app)
    httpd.serve_forever()











