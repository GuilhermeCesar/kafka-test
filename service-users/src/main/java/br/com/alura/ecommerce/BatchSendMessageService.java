package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {


    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:service-users/target/user_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            this.connection.createStatement()
                    .execute("create table  Users (" +
                            " uuid varchar(200) primary key ," +
                            " email varchar(200)" +
                            ") ");

        } catch (SQLException ex) {
            //be careful, the sql could be wrong, be relly careful
            ex.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                String.class,
                Map.of())) {
            service.run();
        }

    }

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();


    private void parse(ConsumerRecord<String, String> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("--------------------------------------------------------------");
        System.out.println("Processing new batch");
        System.out.println("Topic: "+ record.value());

        var order = record.value();

        for (User user : getAllUsers()) {
            userDispatcher.send("USER_GENERATE_READING_REPORT", user.getUuid(), user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select  uuid from Users")
                .executeQuery();

        List<User> users = new ArrayList<>();

        while (results.next()){
            users.add(new User(results.getString(1)));
        }
        return users;
    }
}
