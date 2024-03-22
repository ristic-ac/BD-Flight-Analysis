# BD-Flight-Analysis

# TODO
# BATCH Dodati bar 3 window fun
# Do 3 vizualizovati

# STREAMING sink rezultata upita, window spajanje vise tokova


- Predmet: Arhitekture sistema velikih skupova podataka
- Student: Branislav Ristić
- Tema: Analiza podataka o letovima
- Opis projekta:
    - Cilj projekta je analiza podataka o letovima. Podaci se obrađuju u realnom vremenu i u batch režimu. 
    - Podaci se čuvaju u HDFS-u i MongoDB bazi podataka.
    - Za obradu podataka koristi se Spark.
        - Ukupno 10 upita se izvršava nad podacima.
    - Za streaming obradu podataka koristi se Spark Structured Streaming.
        - Ukupno 5 upita se izvršava nad podacima.
    - Za vizuelizaciju se koristi Metabase.
    - Projekat se pokreće u Docker kontejnerima.

- Struktura repozitorijuma:
    - `batch/` - skripte za batch obradu podataka
    - `config/` - konfiguracioni fajlovi za Hadoop
    - `diagrams/` - dijagrami arhitekture
    - `producer/` - Kafka producer aplikacija
    - `sample_data/` - podaci namenjeni za obradu
    - `scripts/` - skripte za pokretanje obrađivanja podataka
        - `hdfs/` - skripte za rad sa HDFS-om
            - `put.sh` - skripta za uploadovanje podataka na HDFS
        - `kafka/` - skripte za rad sa Kafka-om
            - `create_topics.sh` - skripta za kreiranje Kafka topika
        - `mongo/` - skripte za rad sa MongoDB-om
            - `mongo.sh` - skripta generalnu manipulaciju sa MongoDB-om
        - `spark/` - skripte za rad sa Spark-om
            - `batch/` - skripte za pokretanje batch obrade
                - `queryX.sh` - skripte za pokretanje određenog upita
            - `streaming/` - skripte za pokretanje streaming obrade
                - `queryX.sh` - skripte za pokretanje određenog upita

    - `streaming/` - skripte za streaming obradu podataka
    - `README.md` - opis projekta
    - `docker-compose.yaml` - konfiguracioni fajl za pokretanje klastera
    - `run.sh` - skripta za pokretanje klastera



![Diagram](./diagrams/diagram.drawio.png)

