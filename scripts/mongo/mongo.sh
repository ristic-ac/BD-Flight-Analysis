docker exec -it mongodb bash
mongosh
use flights
db.createCollection("flights_germany")
db.flights_germany.find().pretty()