package br.com.alura.ecommerce;

import br.com.alura.ecommerce.database.LocalDatabase;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OrdersDatabase implements Closeable {

    private final LocalDatabase database;

    public OrdersDatabase() throws SQLException {
        this.database = new LocalDatabase("orders_database");
        //you might want to save all data
        this.database.createIfNotExists("create table  Orders (" +
                " uuid varchar(200) primary key ," +
                " is_fraud boolean" +
                ") ");
    }

    public boolean saveNewOrder(Order order) throws SQLException {
        if (wasProcessed(order)) {
            return false;
        }
        database.update("insert into Orders (uuid) values (?) ", order.getOrderId());
        return true;
    }

    private boolean wasProcessed(Order order) throws SQLException {
        ResultSet resultSet = database.query("select uuid from Orders where uuid = ? limit 1 ", order.getOrderId());
        return resultSet.next();
    }

    @Override
    public void close() throws IOException {
        try {
            database.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
