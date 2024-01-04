docker exec -it mongo bash
mongo
use flights
db.createCollection("flights_germany")
db.flights_germany.find().pretty()