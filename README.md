# Getting Started OpenTracing
- https://opentracing.io
- https://opentracing.io/registry
- https://github.com/yurishkuro/opentracing-tutorial
- https://mvnrepository.com/artifact/io.jaegertracing/jaeger-client
- https://mvnrepository.com/artifact/com.google.guava/guava

# Producer App
- https://github.com/opentracing-contrib/java-kafka-client
- https://mvnrepository.com/artifact/io.opentracing.contrib/opentracing-kafka-client
- https://mvnrepository.com/artifact/io.opentracing.contrib/opentracing-kafka-streams
- https://mvnrepository.com/artifact/io.opentracing.contrib/opentracing-kafka-spring

# Requirements
## Kafka
```shell script
git clone git@github.com:confluentinc/cp-all-in-one.git
cd cp-all-in-one/cp-all-in-one
docker-compose up -d
```

```
http://localhost:9021
```

## jaeger
```shell script
docker run -d -p6831:6831/udp -p16686:16686 jaegertracing/all-in-one:latest
```

```
http://localhost:16686
```