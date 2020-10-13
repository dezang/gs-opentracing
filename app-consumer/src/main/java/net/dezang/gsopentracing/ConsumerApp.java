package net.dezang.gsopentracing;

import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.contrib.kafka.spring.TracingConsumerFactory;
import io.opentracing.tag.Tags;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class ConsumerApp {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApp.class, args);
    }

    @Configuration
    @RequiredArgsConstructor
    static class Config {
        @Value("${spring.application.name}")
        private String applicationName;
        private final KafkaProperties kafkaProperties;

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
        ConsumerFactory<String, String> consumerFactory() {
            return new TracingConsumerFactory<>(new DefaultKafkaConsumerFactory<>(
                    kafkaProperties.buildConsumerProperties()), tracer());
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            return factory;
        }
    }

    @Log4j2
    @Component
    @RequiredArgsConstructor
    static class SimpleConsumer {
        private final Tracer tracer;

        @KafkaListener(topics = "test.tracing")
        public void consume(ConsumerRecord<String, String> record) {
            log.info("received message={}", record.value());
            Headers headers = record.headers();
            SpanContext spanContext = TracingKafkaUtils.extractSpanContext(headers, tracer);
            Span span = tracer.buildSpan("consumed")
                    .asChildOf(spanContext)
                    .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                    .start();
            try (Scope ignored = tracer.scopeManager().activate(span)) {
                work();
            } finally {
                span.finish();
            }
        }
        private void work() {
            Span span = tracer.buildSpan("consumer-work").start();
            try (Scope ignored = tracer.scopeManager().activate(span)) {
                try {
                    System.out.println("working!");
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } finally {
                span.finish();
            }
        }

        @KafkaListener(topics = "test.tracing.stream")
        public void consumeFromStream(ConsumerRecord<String, String> record) {
            log.info("received message={}", record.value());
            Headers headers = record.headers();
            SpanContext spanContext = TracingKafkaUtils.extractSpanContext(headers, tracer);
            Span span = tracer.buildSpan("consumed")
                    .asChildOf(spanContext)
                    .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                    .start();
            try (Scope ignored = tracer.scopeManager().activate(span)) {
                saveToDb();
            } finally {
                span.finish();
            }
        }

        private void saveToDb() {
            Span span = tracer.buildSpan("saveToDb").start();
            try (Scope ignored = tracer.scopeManager().activate(span)) {
                try {
                    System.out.println("saved!");
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } finally {
                span.finish();
            }
        }
    }
}
