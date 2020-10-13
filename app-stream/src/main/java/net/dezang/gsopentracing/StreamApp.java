package net.dezang.gsopentracing;

import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.streams.TracingKafkaClientSupplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

@SpringBootApplication
public class StreamApp {
    public static void main(String[] args) {
        SpringApplication.run(StreamApp.class, args);
    }

    @Configuration
    @RequiredArgsConstructor
    static class Config {
        @Value("${spring.application.name}")
        private String applicationName;

        @Bean
        Tracer tracer() {
            io.jaegertracing.Configuration.SamplerConfiguration samplerConfig = io.jaegertracing.Configuration.SamplerConfiguration.fromEnv().withType(ConstSampler.TYPE).withParam(1);
            io.jaegertracing.Configuration.SenderConfiguration senderConfig = io.jaegertracing.Configuration.SenderConfiguration.fromEnv()
                    .withAgentHost("localhost")
                    .withAgentPort(6831);
            io.jaegertracing.Configuration.ReporterConfiguration reporterConfig = io.jaegertracing.Configuration.ReporterConfiguration.fromEnv().withLogSpans(true).withSender(senderConfig);
            io.jaegertracing.Configuration config = new io.jaegertracing.Configuration(applicationName)
                    .withSampler(samplerConfig)
                    .withReporter(reporterConfig);
            return config.getTracer();
        }

        @Bean
        KafkaClientSupplier kafkaClientSupplier() {
            return new TracingKafkaClientSupplier(tracer());
        }
    }

    @Log4j2
    @Component
    @RequiredArgsConstructor
    static class SimpleStreamer {
        private final Tracer tracer;
        private final KafkaClientSupplier kafkaClientSupplier;
        private final KafkaProperties kafkaProperties;


        @PostConstruct
        public void runStream() {
            Properties streamProperties = new Properties();
            streamProperties.putAll(kafkaProperties.buildStreamsProperties());
            StreamsBuilder streamsBuilder = new StreamsBuilder();

            KStream<String, String> stream = streamsBuilder.stream(
                    "test.tracing", Consumed.with(Serdes.String(), Serdes.String()));

            // TODO extractSpanContext
            // TracingKafkaUtils.extractSpanContext(null, tracer);
            stream.mapValues((key, value) -> work(value))
                    .print(Printed.toSysOut());
            stream.to("test.tracing.stream");

            KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamProperties, kafkaClientSupplier);
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }

        private String work(String message) {
            Span span = tracer.buildSpan("stream-work").start();
            try (Scope ignored = tracer.scopeManager().activate(span)) {
                try {
                    log.info("working!");
                    Thread.sleep(1000);
                    return message + " streamed!";
                } catch (InterruptedException e) {
                    log.warn("Interrupted!", e);
                    Thread.currentThread().interrupt();
                    return message + " exception!";
                }
            } finally {
                span.finish();
            }
        }
    }
}
