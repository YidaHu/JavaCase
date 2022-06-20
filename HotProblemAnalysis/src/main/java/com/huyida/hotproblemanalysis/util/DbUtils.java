package com.huyida.hotproblemanalysis.util;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @program: HotProblemAnalysis
 * @description:
 * @author: huyida
 * @create: 2022-06-20 20:19
 **/

public class DbUtils {
    public static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/hotproblem?useSSL=false", "root", "123456");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }
}
