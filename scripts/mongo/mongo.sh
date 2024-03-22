docker exec -it mongodb bash
mongosh
use flights
db.createCollection("flights_germany")
db.flights_germany.find().pretty()
# After creating database and collection, account for metabase should be created
use admin
db.createUser(
  {
    user: "metabase",
    pwd: "metabase",
    roles: [ { role: "readWrite", db: "flights" } ]
  }
)
# Check if the user was created
db.getUsers()