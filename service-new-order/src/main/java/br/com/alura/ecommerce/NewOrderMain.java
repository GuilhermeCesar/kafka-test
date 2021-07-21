package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var userId = UUID.randomUUID().toString();
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            for (int i = 0; i < 10; i++) {

                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                var order = new Order(userId, orderId, amount);
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

            }
        }

        try (var emailDispatcher = new KafkaDispatcher<String>()) {
            for (int i = 0; i < 10; i++) {
                var email = "Thank you for your order! We are processing your order!";
                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
            }
        }
    }


}