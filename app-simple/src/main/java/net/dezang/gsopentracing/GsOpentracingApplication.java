package net.dezang.gsopentracing;

import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.opentracing.Span;
import io.opentracing.Tracer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@RequiredArgsConstructor
@SpringBootApplication
public class GsOpentracingApplication implements ApplicationRunner {
    private final Tracer tracer;

    public static void main(String[] args) {
        SpringApplication.run(GsOpentracingApplication.class, args);
    }

    private void sayHello(String helloTo) {
        Span span = tracer.buildSpan("say-hello").start();
        span.setTag("hello-to", helloTo);

        String helloStr = String.format("Hello, %s!", helloTo);
        System.out.println(helloStr);

        span.finish();
    }

    @Override
    public void run(ApplicationArguments args) {
        if (args.getNonOptionArgs().size() != 1) {
            throw new IllegalArgumentException("Expecting one argument");
        }

        String helloTo = args.getNonOptionArgs().get(0);
        sayHello(helloTo);
    }

    @Configuration
    static class Config {
        @Bean
        Tracer tracer() {
            SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv().withType("const").withParam(1);
            ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv().withLogSpans(true);
            io.jaegertracing.Configuration config = new io.jaegertracing.Configuration("app-simple")
                    .withSampler(samplerConfig)
                    .withReporter(reporterConfig);
            return config.getTracer();
        }
    }
}
