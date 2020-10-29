from flask import Flask
import os
from elasticapm.contrib.flask import ElasticAPM
import logging
from flask import jsonify, request
import json
from datetime import datetime
from datetime import timezone
import requests

logging.basicConfig(level=logging.INFO)
elastic_apm = ElasticAPM()

success_response_object = {"status": "success"}
success_code = 202
failure_response_object = {"status": "failure"}
failure_code = 400

def get_ds_id(thing, sensor):
    """
    requests the datastream id corresponding to the thing and sensor links given
    returns -1 if not found
    """
    payload = {"thing": thing, "sensor": sensor}
    logging.debug(f"getting datastream id {payload}")
    resp = requests.get("http://st_datastreams_api:4999/datastream", params=payload)
    #resp = requests.get("http://host.docker.internal:1338/datastream", params=payload)
    # print(resp.json())
    logging.debug(f"response: {resp.json()} ")

    ds = resp.json()["Datastreams"]
    if len(ds) == 1:
        return ds[0]["datastream_id"]
    else:
        return -1


def create_app(script_info=None):

    # instantiate the app
    app = Flask(__name__)

    # set config
    app_settings = os.getenv("APP_SETTINGS")
    app.config.from_object(app_settings)

    # set up extensions
    elastic_apm.init_app(app)
    # db.init_app(app)

    # shell context for flask cli
    @app.shell_context_processor
    def ctx():
        return {"app": app}

    @app.route("/")
    def hello_world():
        return jsonify(health="ok")

    @app.route("/viikkisolar/observation", methods=["POST"])
    def post_solarinverter_data():
        try:
            data = request.get_json()
            #uncomment for prod
            data = json.loads(data)
            # print(data)
            logging.info(f"post observation: {data}")

            topic_prefix = "finest-observations-viikkisolar"

            thing = f"ViikkiSolar-{data['name']}"
            timestamp = data["timestamp"]
            dt_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
            phenomenon_timestamp_millisec = round(dt_obj.timestamp() * 1000)
            dt_obj = datetime.utcnow()
            result_timestamp_millisec = round(dt_obj.timestamp() * 1000)


            del data["name"]
            del data["timestamp"]
            del data["type"]

            for key, value in data.items():
                sensor = key
                # handle status status not 200 - if not raise error
                ds_id = get_ds_id(thing, sensor)
                if ds_id == -1:
                    logging.warning(f"no datastream id found for {thing} + {sensor}")
                topic = f"{topic_prefix}"
                observation = {
                    "phenomenontime_begin": phenomenon_timestamp_millisec,
                    "phenomenontime_end": None,
                    "resulttime": result_timestamp_millisec,
                    "result": f"{value}",
                    "resultquality": None,
                    "validtime_begin": None,
                    "validtime_end": None,
                    "parameters": None,
                    "datastream_id": ds_id,
                    "featureofintrest_link": None,
                }
                logging.info(observation)

                payload = {"topic": topic, "observation": observation}

                headers = {"Content-type": "application/json"}
                resp = requests.post(
                    "http://st_observations_api:4888/observation",
                    data=json.dumps(payload),
                    headers=headers,
                )
                #resp = requests.post("http://host.docker.internal:1337/observation", data=json.dumps(payload), headers=headers)

            return success_response_object, success_code

        except Exception as e:
            logging.error("Error at %s", "data to kafka", exc_info=e)
            return failure_response_object, failure_code

    return app
