docker exec -it mongodb bash
mongosh
use flights
db.createCollection("flights_germany")
use admin
db.createUser(
  {
    user: "metabase",
    pwd: "metabase",
    roles: [ { role: "readWrite", db: "flights" } ]
  }
)
db.getUsers()

# URL: mongodb
# Database Name: flights
# Port: 27017
# Username: metabase
# Password: metabase
# Authentication Database Name: admin

# Collection Name: flights_germany