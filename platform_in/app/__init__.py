from flask import Flask
import os
from elasticapm.contrib.flask import ElasticAPM
import logging
from flask import jsonify, request
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import certifi
#from flask_sqlalchemy import SQLAlchemy
from datetime import datetime



logging.basicConfig(level=logging.INFO)
elastic_apm = ElasticAPM()

#db = SQLAlchemy()

success_response_object = {"status":"success"}
success_code = 202
failure_response_object = {"status":"failure"}
failure_code = 400

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def kafka_avro_produce(avroProducer, topic, data):

    try:
        avroProducer.produce(topic=topic, value=data)
        logging.debug("avro produce")
        avroProducer.poll(2)
        if len(avroProducer) != 0:
            return False
    except BufferError:
        logging.error("local buffer full", len(avroProducer))
        return False
    except Exception as e:
        logging.error(e)
        return False

    return True

def get_ds_id(thing,sensor):

    things = ["Inv1","Inv2","Inv3","Inv4","Inv5","Inv6","Inv7","Inv8"]
    sensors = [ "VoltagePhase1","VoltagePhase2", "VoltagePhase3",

     "VoltageString1",
  "CurrentString1",
  "OutputString1",
  "VoltageString2",
  "CurrentString2",
  "OutputString2",
  "VoltageString3",
  "CurrentString3",
  "OutputString3",

  "CurrentPhase1",
  "OutputPhase1",

  "CurrentPhase2",
  "OutputPhase2",

"CurrentPhase3",
"OutputPhase3",
"TotalEnergy",
"DailyEnery",
"Status",
"Fault"]

    datastreams = list([(a, b) for a in things for b in sensors])
    return datastreams.index((thing,sensor))+89

def create_app(script_info=None):

    # instantiate the app
    app = Flask(__name__)

    # set config
    app_settings = os.getenv("APP_SETTINGS")
    app.config.from_object(app_settings)

    # set up extensions
    elastic_apm.init_app(app)
    #db.init_app(app)

    value_schema = avro.load("avro/observation.avsc")
    avroProducer = AvroProducer(
        {
            "bootstrap.servers": app.config["KAFKA_BROKERS"],
            "security.protocol": app.config["SECURITY_PROTOCOL"],
            "sasl.mechanism": app.config["SASL_MECHANISM"],
            "sasl.username": app.config["SASL_UNAME"],
            "sasl.password": app.config["SASL_PASSWORD"],
            "ssl.ca.location": certifi.where(),
            #"debug": "security,cgrp,fetch,topic,broker,protocol",
            "on_delivery": delivery_report,
            "schema.registry.url": app.config["SCHEMA_REGISTRY_URL"]
        },
        default_value_schema=value_schema,
    )

    # shell context for flask cli
    @app.shell_context_processor
    def ctx():
        return {"app": app}

    @app.route("/")
    def hello_world():
        return jsonify(health="ok")

    @app.route('/viikkisolar/observation', methods=['POST'])
    def post_solarinverter_data():
        try:
            data = request.get_json()

            #data = json.loads(data)
            #print(data)
            logging.debug(f"post observation: {data}")
            #print("post data for solar inverter", data)

            topic_prefix = "finest-observations-viikkisolar"

            thing = data["name"]
            timestamp = data["timestamp"]
            dt_obj = datetime.strptime('2020-04-20T17:56:50.093538',
                           '%Y-%m-%dT%H:%M:%S.%f')
            timestamp_millisec = round(dt_obj.timestamp() * 1000)
            del data["name"]
            del data["timestamp"]
            del data["type"]

    #save type - name and time stampd
    # for in keys
    #query with inverter 1 and voltage string 1
    #avro produce to obs with id
            print(data)
            for key,value in data.items():
                sensor = key
                print(sensor)
                ds_id = get_ds_id(thing,sensor)
                topic = f"{topic_prefix}"
                observation = {
                "phenomenontime_begin":None,
                "phenomenontime_end":None,
                "resulttime":timestamp_millisec,
                "result":f"{value}",
                "resultquality":None,
                "validtime_begin":None,
                "validtime_end":None,
                "parameters":None,
                "datastream_id":ds_id,
                "featureofintrest_link":None,}

                print(observation)
                kafka_avro_produce(avroProducer, topic,observation)
            return success_response_object,success_code

        except Exception as e:
            avroProducer.flush()
            print("solar inverter error", e)
            return failure_response_object,failure_code

    return app


