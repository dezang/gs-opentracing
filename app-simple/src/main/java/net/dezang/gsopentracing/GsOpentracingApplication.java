package net.dezang.gsopentracing;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GsOpentracingApplication implements ApplicationRunner {
    public static void main(String[] args) {
        SpringApplication.run(GsOpentracingApplication.class, args);
    }

    private void sayHello(String helloTo) {
        String helloStr = String.format("Hello, %s!", helloTo);
        System.out.println(helloStr);
    }

    @Override
    public void run(ApplicationArguments args) {
        if (args.getNonOptionArgs().size() != 1) {
            throw new IllegalArgumentException("Expecting one argument");
        }

        String helloTo = args.getNonOptionArgs().get(0);
        sayHello(helloTo);
    }
}
