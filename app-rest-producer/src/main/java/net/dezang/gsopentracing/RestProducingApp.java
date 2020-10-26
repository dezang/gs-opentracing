package net.dezang.gsopentracing;

import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.spring.TracingProducerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@EnableScheduling
@SpringBootApplication
public class RestProducingApp {
    public static void main(String[] args) {
        SpringApplication.run(RestProducingApp.class, args);
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
        ProducerFactory<String, String> producerFactory() {
            return new TracingProducerFactory<>(new DefaultKafkaProducerFactory<>(
                    kafkaProperties.buildProducerProperties()), tracer());
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }
    }

    @RestController
    @RequiredArgsConstructor
    @Log4j2
    static class SimpleRestProducer {
        private final KafkaTemplate<String, String> kafkaTemplate;

        @GetMapping("send")
        public ResponseEntity<?> produce(@RequestParam String message) {
            log.info(message);
            kafkaTemplate.send("test.tracing", message + " by rest producer");
            return ResponseEntity.ok(message);
        }
    }
}
