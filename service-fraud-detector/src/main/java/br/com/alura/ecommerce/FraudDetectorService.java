package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudeService = new FraudDetectorService();
        try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudeService::parse,
                Order.class, new HashMap<>())) {
            service.run();
        }

    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();


    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        var order = record.value();
        if (isFraud(order)) {
            //pretending that the fraud happens when the amonunt is >=4500
            System.out.println("Order is a fraud !!!!!" + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APROVED", order.getUserId(), order);
        }

    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        return properties;
    }
}