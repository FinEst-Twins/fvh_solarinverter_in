
To bring up service:

Note: the environment variables file config.env must be at root folder and the ssl root cert (pem file) should be in folder /platform_in

dev config (flask dev server):

    docker-compose up
    send PUT requests to localhost:5000/viikkisolar/observation with solar inverter data
    should return success or failure response

prod config (nginx+gunicorn)

    docker-compose -f docker-compose.prod.yml up

2,3,4 same as previous but port for prod is 1337


POST data example:
```json
{
	"name": "Inv6",
	"timestamp": "2020-04-20T17:56:50.093538",
	"type": "Pico 10 solar inverter",
	"VoltageString1": 0.0,
	"CurrentString1": 0.0,
	"OutputString1": 0,
	"VoltageString2": 0.0,
	"CurrentString2": 0.0,
	"OutputString2": 0,
	"VoltageString3": 0.0,
	"CurrentString3": 0.0,
	"OutputString3": 0,
	"VoltagePhase1": 0.0,
	"CurrentPhase1": 0.0,
	"OutputPhase1": 0,
	"VoltagePhase2": 0.0,
	"CurrentPhase2": 0.0,
	"OutputPhase2": 0,
	"VoltagePhase3": 0.0,
	"CurrentPhase3": 0.0,
	"OutputPhase3": 0,
	"TotalEnergy": 35731981,
	"DailyEnery": 17118,
	"Status": 0,
	"Fault": 0
}
