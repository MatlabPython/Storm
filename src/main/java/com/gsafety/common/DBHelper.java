package com.gsafety.common;

import com.gsafety.storm.SystemConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @Author: yifeng G
 * @Date: Create in 13:13 2018/6/4 2018
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public class DBHelper {

    //静态代码块负责加载驱动
    static {
        try {
            Class.forName(SystemConfig.get("MYSQL_DRIVER"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    //单例模式返回数据库连接对象，供外部调用
    public static Connection getConnection() throws Exception {
        Connection conn = DriverManager.getConnection(SystemConfig.get("MYSQL_URL"), SystemConfig.get("MYSQL_USERNAME"), SystemConfig.get("MYSQL_PASSWORD"));
        return conn;

    }

    public static void close() throws Exception {
        getConnection().close();
    }

    public static Statement createStatement() throws Exception {
        return getConnection().createStatement();
    }

    //写main方法测试是否连接成功，可将本类运行为Java程序先进行测试，再做后续的数据库操作。
    public static void main(String[] args) {

        try {
            Connection conn = DBHelper.getConnection();
            if (conn != null) {
                System.out.println("数据库连接正常！");
            } else {
                System.out.println("数据库连接异常！");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
