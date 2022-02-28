import pymongo
from pymongo import MongoClient, UpdateOne
import pandas as pd 
import sys
import json
# Zbytečné přenášet posem - db, coll varables

from flask import Flask, request, abort
from flask import Flask
from flask_restful import Resource, Api, reqparse
from marshmallow import Schema, fields
import pandas as pd
import dbFunc

# // Source - https://towardsdatascience.com/the-right-way-to-build-an-api-with-python-cd08ab285f8f (PUT, POST taky)

client = MongoClient("mongodb+srv://analytics:analytics-password@mflix.i8krg.mongodb.net")
db = client["CoinMonitor"]
collectionName = "result"
numberOfRecords = 100
timeFrame = int(1440)

app = Flask(__name__)
api = Api(app)

# MARSHMALLOW - https://stackoverflow.com/questions/30779584/flask-restful-passing-parameters-to-get-request
class BarQuerySchema(Schema):
    style = fields.Str(required=True)
    timeFrame = fields.Str(required=True)

app = Flask(__name__)
api = Api(app)
schema = BarQuerySchema()

class dataForPlot(Resource):
    def get(self):
            client = MongoClient("mongodb+srv://analytics:analytics-password@mflix.i8krg.mongodb.net")
            db = client["CoinMonitor"]
            collForInsert = db["result"]
            invTypes = ["small", "medium", "large", "huge"]

            errors = schema.validate(request.args)
            if errors:
                abort(400, str(errors))
            
            style = request.args["style"]
            timeFrame = request.args["timeFrame"]

            filter = {
                        "timeFrame": int(timeFrame)
                    }

            existingRecord = list(collForInsert.find(filter))
            
            DF3 = pd.json_normalize(existingRecord)
            DF3.drop("_id", axis=1, inplace=True)

            Dict3 = DF3.to_dict("records")

            if style == "candle":

                DFOut = DF3[["timeFrameStart", "highPrice", "lowPrice", "openPrice", "closePrice"]]
                DictOut = DFOut.to_dict("records")
                
            elif style == "total":

                DFOut = DF3[["timeFrameStart", "volumeTraded"]]
                DictOut = DFOut.to_dict("records")

            elif style in invTypes:

                DFOut = DF3[["timeFrameStart", plotType+".investorCountQty", plotType+".investorMeanQty"]]
                DictOut = DFOut.to_dict("records")

            else:

                DictOut = DF3.to_dict("records")

            print(style)
            print(timeFrame)
            return DictOut, 200

    
api.add_resource(dataForPlot, '/dataForPlot')

if __name__ == '__main__':
    app.run()

@parser.error_handler
def handle_request_parsing_error(err, req, schema, *, error_status_code, error_headers):
    abort(error_status_code, errors=err.messages)

if __name__ == '__main__':
    app.run(debug=True)