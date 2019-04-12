# influxer

## Usage

Startup the environment:

```sh
docker-compose up
```

Startup a netcat server in another terminal:

```sh
docker-compose exec netcat nc -l 9000
```

Build and run the Flink job:

```sh
mvn clean package -DskipTests

docker-compose exec jobmanager \
bash -c "flink run -d /influxer/target/influxer-standalone.jar 9000"
```

You can check its status in the [Flink UI](http://localhost:8081).

Once the job is running, type some numbers from the netcat server terminal:

```
22
24
23
```

Finally, check the resulting 10-second windowed sums in InfluxDB:

```sh
docker-compose exec influxdb \
influx -database data -execute 'select * from record``s'
```

Stop the environment:

```sh
docker-compose down
```

