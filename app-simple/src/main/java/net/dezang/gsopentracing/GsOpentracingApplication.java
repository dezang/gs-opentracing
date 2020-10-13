package net.dezang.gsopentracing;

import com.google.common.collect.ImmutableMap;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Scope;
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

    @Configuration
    static class Config {
        @Bean
        Tracer tracer() {
            SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv().withType(ConstSampler.TYPE).withParam(1);
            io.jaegertracing.Configuration.SenderConfiguration senderConfig = io.jaegertracing.Configuration.SenderConfiguration.fromEnv()
                    .withAgentHost("localhost")
                    .withAgentPort(6831);
            ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv().withLogSpans(true).withSender(senderConfig);
            io.jaegertracing.Configuration config = new io.jaegertracing.Configuration("app-simple")
                    .withSampler(samplerConfig)
                    .withReporter(reporterConfig);
            return config.getTracer();
        }
    }

    @Override
    public void run(ApplicationArguments args) {
        if (args.getNonOptionArgs().size() != 1) {
            throw new IllegalArgumentException("Expecting one argument");
        }

        String helloTo = args.getNonOptionArgs().get(0);
        sayHello(helloTo);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void sayHello(String helloTo) {
        Span span = tracer.buildSpan("say-hello").start();
        try (Scope ignored = tracer.scopeManager().activate(span)) {
            span.setTag("hello-to", helloTo);

            String helloStr = formatString(helloTo);
            printHello(helloStr, span);
        } finally {
            span.finish();
        }
    }

    private String formatString(String helloTo) {
        Span span = tracer.buildSpan("formatString").start();
        try (Scope ignored = tracer.scopeManager().activate(span)) {
            span.log(ImmutableMap.of("event", "string-format", "value", helloTo));
            return String.format("Hello, %s!", helloTo);
        } finally {
            span.finish();
        }
    }

    private void printHello(String helloStr, Span rootSpan) {
        Span span = tracer.buildSpan("printHello")
                .asChildOf(rootSpan)
                .start();
        try (Scope ignored = tracer.scopeManager().activate(span)) {
            span.log(ImmutableMap.of("event", "println"));
            System.out.println(helloStr);
        } finally {
            span.finish();
        }
    }
}
