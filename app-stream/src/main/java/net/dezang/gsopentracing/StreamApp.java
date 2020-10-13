package net.dezang.gsopentracing;

import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.contrib.kafka.streams.TracingKafkaClientSupplier;
import io.opentracing.util.GlobalTracer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.function.Function;

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

        @PostConstruct
        public void registerToGlobalTracer() {
            if (!GlobalTracer.isRegistered()) {
                GlobalTracer.registerIfAbsent(tracer());
            }
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

            Serde<String> stringSerde = Serdes.String();
            KStream<String, String> stream = streamsBuilder.stream(
                    "test.tracing", Consumed.with(stringSerde, stringSerde));

            stream.transform(
                    () -> new TransformerWithTracing<String, String>("work", new WorkFunction()))
                    .to("test.tracing.stream", Produced.with(stringSerde, stringSerde));

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

    static class WorkFunction implements Function<String, String> {

        @Override
        public String apply(String s) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return s + "worked";
        }

        @Override
        public <V> Function<V, String> compose(Function<? super V, ? extends String> before) {
            return null;
        }

        @Override
        public <V> Function<String, V> andThen(Function<? super String, ? extends V> after) {
            return null;
        }
    }

    @Log4j2
    static class TransformerWithTracing<V, VR> implements Transformer<String, V, KeyValue<String, VR>> {
        private ProcessorContext context;
        final Function valueAction;
        final String operatorName;

        public TransformerWithTracing(String operatorName, Function valueAction) {
            this.operatorName = operatorName;
            this.valueAction = valueAction;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, VR> transform(String key, V value) {
            Tracer tracer = GlobalTracer.get();
            Headers headers = context.headers();
            SpanContext spanContext = TracingKafkaUtils.extractSpanContext(headers, tracer);
            Span span = tracer.buildSpan(operatorName).asChildOf(spanContext).start();
            try (Scope ignored = tracer.scopeManager().activate(span)) {
                return KeyValue.pair(key, (VR) valueAction.apply(value));
            } finally {
                span.finish();
            }
        }

        @Override
        public void close() {

        }
    }
}
