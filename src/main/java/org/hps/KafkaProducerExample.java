package org.hps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.time.Instant.now;

public class KafkaProducerExample {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);
    private static long iteration = 0;

    private static KafkaProducerConfig config;
    private static KafkaProducer<String, Customer> producer;
    private static Random rnd;
    private static long key;
    private static int eventsPerSeconds;

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        rnd = new Random();
        config = KafkaProducerConfig.fromEnv();
        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        producer = new KafkaProducer<String, Customer>(props);
        log.info("Sending {} messages ...", config.getMessageCount());

        AtomicLong numSent = new AtomicLong(0);
        // over all the workload
        key = 0L;


        tenEventsPerSecForOneMinute();
        tenEventsPerSecForOneMinute();
        //TwentyEventsPerSecForOneMinute();
        //TwentyEventsPerSecForOneMinute();
        ThirtyEventsPerSecForOneMinute();
        ThirtyEventsPerSecForOneMinute();

       /* FourteeEventsPerSecForOneMinute();
        increase1EventPerSecFor1min();
        remainConstantFor1min();
        increase1EventPerSecFor2min();
        remainConstant();*/
    }


    static void tenEventsPerSecForOneMinute() throws InterruptedException {
        eventsPerSeconds = 10;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }

        log.info("End sending 10 events per sec for One Minute");
        log.info("==========================================");
    }


    static void TwentyEventsPerSecForOneMinute() throws InterruptedException {
        eventsPerSeconds = 20;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }
    }

    static void ThirtyEventsPerSecForOneMinute() throws InterruptedException {
        eventsPerSeconds = 30;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }
    }

    static void FourteeEventsPerSecForOneMinute() throws InterruptedException {
         eventsPerSeconds = 40;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for {} seconds", 1000);
            Thread.sleep(1000);
            end = now();
        }

        log.info("End sending 30 events per sec for One Minute");
        log.info("==========================================");

    }

    static void increase1EventPerSecFor1min() throws InterruptedException {
        eventsPerSeconds = 41;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for one seconds ");
            Thread.sleep(1000);
            eventsPerSeconds++;
            end = now();
        }
        log.info("End sending increase linearly  for One Minute");
        log.info("==========================================");
    }


    static void remainConstantFor1min() throws InterruptedException {
        log.info("From now on I am remaining constant with {} events per sec", eventsPerSeconds);

        Instant start = now();
        Instant end = now();

        while (Duration.between(start, end).toMinutes() <= 0) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                // log.info("Sending the following customer {}",  custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for 1 second ");
            Thread.sleep(1000);
            end = now();
        }
    }


    static void increase1EventPerSecFor2min() throws InterruptedException {
        eventsPerSeconds = 100;
        Instant start = now();
        Instant end = now();
        while (Duration.between(start, end).toMinutes() <= 1) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                //log.info("Sending the following customer {}", custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for one seconds ");
            Thread.sleep(1000);
            eventsPerSeconds++;
            end = now();
        }
        log.info("End sending increase linearly  for One Minute");
        log.info("==========================================");
    }

    static void remainConstant() throws InterruptedException {
        log.info("From now on I am remaining constant with {} events per sec", eventsPerSeconds);


        while (true) {
            for (int j = 0; j < eventsPerSeconds; j++) {
                Customer custm = new Customer(rnd.nextInt(), UUID.randomUUID().toString());
                producer.send(new ProducerRecord<String, Customer>(config.getTopic(),
                        null, null, UUID.randomUUID().toString(), custm));
                // log.info("Sending the following customer {}",  custm.toString());
            }
            log.info("sent {} eventsPerSeconds", eventsPerSeconds);
            log.info("sleeping for 1 second ");
            Thread.sleep(1000);
        }

    }
}








