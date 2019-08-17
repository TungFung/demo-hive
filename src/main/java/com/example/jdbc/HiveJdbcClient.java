package com.example.jdbc;

import java.sql.*;

/**
 * Created by tangweiqun on 2017/10/26.
 */
public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://master:10000", "hadoop-twq", "");
        Statement stmt = con.createStatement();
        String tableName = "twq.tracker_session";
        stmt.execute("CREATE DATABASE IF NOT EXISTS twq");
        stmt.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (\n" +
                " session_id string,\n" +
                " session_server_time string,\n" +
                " cookie string,\n" +
                " cookie_label string,\n" +
                " ip string,\n" +
                " landing_url string,\n" +
                " pageview_count int,\n" +
                " click_count int,\n" +
                " domain string,\n" +
                " domain_label string)\n" +
                "STORED AS PARQUET");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // load data into table
        sql = "LOAD DATA INPATH 'hdfs://master:9999/user/hadoop-twq/example/trackerSession'" + " OVERWRITE INTO TABLE " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getString(2));
        }

        // regular hive query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

        res.close();
        stmt.close();
        con.close();
    }
}
