## Installazione


Installare `maven` e `java 8`

Scaricare `flink 1.2.0`

## Configurazione

Modificare in `src/main/java/control/AppConfigurator.java` il percorso nell'attributo `FILENAME` specificando il percorso di un file generato nella seguente maniera:

---------------------------------------------------------------------------------------------------------------------

###### milliseconds time
WATERMARK_INTERVAL = 1000

###### tuple for testing
DATASET_FILE = /home/ec2-user/dataset.json
NUMBER_OF_CONTACT = 1000

###### zookeeper host & kafka broker
CONSUMER_ZOOKEEPER_HOST = 54.161.196.192:2181
CONSUMER_KAFKA_BROKER = 54.161.196.192:9092
PRODUCER_KAFKA_BROKER = 54.152.81.214:9092

###### topic
LAMP_DATA_TOPIC = lamp_data
RANK_TOPIC = rank
WARNING_HOUR_TOPIC = warning_hour
WARNING_DAY_TOPIC = warning_day
WARNING_WEEK_TOPIC = warning_week
WARNING_STATE = warning_state
HOUR_LAMP_CONS = hour_lamp_cons
DAY_LAMP_CONS = day_lamp_cons
WEEK_LAMP_CONS = week_lamp_cons
HOUR_STREET_CONS = hour_street_cons
DAY_STREET_CONS = day_street_cons
WEEK_STREET_CONS = week_street_cons
HOUR_CITY_CONS = hour_city_cons
DAY_CITY_CONS = day_city_cons
WEEK_CITY_CONS = week_city_cons
MEDIAN_TOPIC = median

###### rank
MAX_RANK_SIZE = 5

###### minimal time for computation of residual lifetime rank - milliseconds time
THRESHOLD = 7889400000

###### seconds time
RANK_WINDOW_SIZE = 10

###### avg consumption - seconds time
HOUR_CONS_WINDOW_SIZE = 3600
HOUR_CONS_WINDOW_SLIDE = 600
DAY_CONS_WINDOW_SIZE = 86400
DAY_CONS_WINDOW_SLIDE = 14400
WEEK_CONS_WINDOW_SIZE = 604800
WEEK_CONS_WINDOW_SLIDE = 86400

###### median - seconds time
MEDIAN_WINDOW_SIZE = 10
MEDIAN_WINDOW_SLIDE = 5

###### warning
WARNING_RATIO = 2.5




---------------------------------------------------------------------------------------------------------------------

## Avvio

Una volta configurata tramite il file di configurazione l'applicazione può essere eseguita in due differenti modalità:

	- tramite IDE (IntelliJ - Eclipse)

	- creare il JAR del progetto tramite il comando `mvn install` eseguito da terminale nella directory contenente il file `pom.xml`

		- eseguire lo script `start-local.sh` nella directory `bin` di Flink per avviare la UI di Flink, contattabile tramite browser alla porta 
		  8081. 

		- è possibile lanciare la propria applicazione in due modi differenti:

			1) tramite UI nella sezione `Submit new Job` -> Add new -> Upload -> Selezionare il giusto JAR -> Entry Class: core.MonitoringApp -> 	Parallelism: X (1-2-3-4) -> Submit

			2) tramite CLI eseguendo `flink` nella directory `bin` di Flink, tramite il comando `flink run -c core.MonitoringApp /path/to/jar/file.jar`

