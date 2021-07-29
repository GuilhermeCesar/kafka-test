package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try (var orderDispatcher = new KafkaDispatcher<>()) {
            try (var emailDispatcher = new KafkaDispatcher<>()) {
                var email = Math.random() + "@email.com";
                for (int i = 0; i < 10; i++) {

                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                    var order = new Message<>(new CorrelationId("NewOrderMain"), new Order(orderId, amount, email));

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
                            order.getId()
                            , order);

                    var emailCode = "Thank you for your order! We are processing your order!";
                    var messageEmail = new Message<>(new CorrelationId("NewOrderMain"), emailCode);
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, messageEmail.getId(), emailCode);

                }
            }
        }
    }
}
