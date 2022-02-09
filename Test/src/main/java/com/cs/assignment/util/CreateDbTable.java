package com.cs.assignment.util;

import com.cs.assignment.entity.Event;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CreateDbTable {
    private static final Logger logger = Logger.getLogger(CreateDbTable.class.getName());
    private static final String createTableSQL = "create table IF NOT EXISTS event (\r\n" + "  eventId  varchar(25) primary key NOT NULL,\r\n" +
            "  duration int(10),\r\n" + "  host varchar(10),\r\n" + "  alert BOOLEAN DEFAULT FALSE,\r\n" +
            "  );";

    private static final String INSERT_USERS_SQL = "INSERT INTO event (eventId, duration, host, alert) \" +\n" +
            "    \" Values (?, ?, ?, ?)";


    public static void createTable() {
        logger.info("Table event does not exist : " + createTableSQL);
        try (Connection connection = JDBCUtil.getConnection();
             Statement statement = connection.createStatement();) {
            statement.execute(createTableSQL);
        } catch (SQLException e) {
            JDBCUtil.printSQLException(e);
        }
    }

    public static void insertData(Event event) throws SQLException {
        createTable();
        try (Connection connection = JDBCUtil.getConnection();

             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_USERS_SQL)) {
            logger.info("Insert data to table event " + INSERT_USERS_SQL);
            preparedStatement.setString(1, event.getId());
            preparedStatement.setInt(2, event.getDuration());
            preparedStatement.setString(3, event.getHost());
            preparedStatement.setBoolean(4, event.isAlert());

            System.out.println(preparedStatement);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Data insertion failed : " + e.getMessage());
            JDBCUtil.printSQLException(e);
        }

    }
}
