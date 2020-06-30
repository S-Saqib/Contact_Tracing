/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author Saqib
 */
public class DbInterface {
    private final String dbUrl, dbUser, dbPassword;
    Properties properties;
    private Connection conn;

    public DbInterface(String dbLocation) {
        dbUrl = "jdbc:postgresql://" + dbLocation + ":5432/contact_tracing";
        dbUser = "contact_tracing";
        dbPassword = "datalabctq";
        properties = new Properties();
        properties.setProperty("user","contact_tracing");
        properties.setProperty("password","datalabctq");
        // properties.setProperty("sslmode","require");    // ssl may be turned off
        conn = null;
    }
    
    public Connection getConnection() throws SQLException{
        if (conn == null || conn.isClosed()){
            conn = DriverManager.getConnection(dbUrl, properties);
            conn.setAutoCommit(false);
        }
        return conn;
    }
    
    public void freeConnection() throws SQLException{
        if (conn != null && !conn.isClosed()){
            conn.close();
        }
    }
    
    public void commit() throws SQLException{
        if (conn == null || conn.isClosed()){
            System.out.println("Connection not open, cannot commit");
        }
        else{
            conn.commit();
        }
    }
}
