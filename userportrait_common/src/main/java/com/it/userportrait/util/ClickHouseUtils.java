package com.it.userportrait.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClickHouseUtils {

    public static void insertData(String tableName, Map<String,String> data, Set<String> intFieldSet){
        String id = System.currentTimeMillis()+"";
        data.put("id",id);
        intFieldSet.add("id");
        String timedate = null;
        try {
            timedate = DateUtils.tranferDate(data.get("timeinfoString").substring(0,8),"yyyyMMdd");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        data.put("timeinfodate",timedate);
        /**
         * insert into youfantest (id,name,create_date) values(1,'xiaobai','2018-09-07');
         */
        String resultsql = "insert into ";
        resultsql += tableName +" (";
        String valuesql = "(";
        Set<Map.Entry<String,String>> sets =  data.entrySet();
        for(Map.Entry<String,String> map:sets){
            String fieldName = map.getKey();
            Object valuestring = map.getValue();
            resultsql += fieldName  +",";
            if(!intFieldSet.contains(fieldName)){
                valuesql += "'"+valuestring + "'" +",";
            }else {
                valuesql += valuestring  +",";
            }

        }
        resultsql = resultsql.substring(0,resultsql.length()-1)+")";
        valuesql = valuesql.substring(0,valuesql.length()-1)+")";
        resultsql = resultsql + " values "+ valuesql;
        System.out.println(resultsql);
        try {
            Connection connection = getConnection("jdbc:clickhouse://192.168.2.111:8123/test","ru.yandex.clickhouse.ClickHouseDriver");
            Statement statement = connection.createStatement();
            statement.execute(resultsql);//执行sql语句
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static Connection getConnection(String addressParam,String driverClassNameParam) throws Exception {
        String address = addressParam;
        Class.forName(driverClassNameParam);
        Connection connection  = DriverManager.getConnection(address);
        return connection;
    }

    public static ResultSet getQueryResult(String database, String sql) throws Exception {
        Connection connection = getConnection("jdbc:clickhouse://hadoop-101:8123/"+database,"ru.yandex.clickhouse.ClickHouseDriver");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        return resultSet;
    }


    public static void insertData2(String tableName, Map<String,String> data, Set<String> intFieldSet){
        /**
         * insert into youfantest (id,name,create_date) values(1,'xiaobai','2018-09-07');
         */
        String resultsql = "insert into ";
        resultsql += tableName +" (";
        String valuesql = "(";
        Set<Map.Entry<String,String>> sets =  data.entrySet();
        for(Map.Entry<String,String> map:sets){
            String fieldName = map.getKey();
            String valuestring = map.getValue();
            resultsql += fieldName  +",";
            if(!intFieldSet.contains(fieldName)){
                valuesql += "'"+valuestring + "'" +",";
            }else {
                valuesql += valuestring  +",";
            }

        }
        resultsql = resultsql.substring(0,resultsql.length()-1)+")";
        valuesql = valuesql.substring(0,valuesql.length()-1)+")";
        resultsql = resultsql + " values "+ valuesql;
        System.out.println(resultsql);
        try {
            Connection connection = getConnection("jdbc:clickhouse://hadoop-101:8123/huaxiang","ru.yandex.clickhouse.ClickHouseDriver");
            Statement statement = connection.createStatement();
            statement.execute(resultsql);//执行sql语句
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        Map<String,String> data = new HashMap<String,String>();
        Set<String> intFieldSet = new HashSet<String>();
        intFieldSet.add("id");
        data.put("id","111");
        data.put("name","xiaobai");
        data.put("create_date","2018-09-07");
        insertData2("test",data,intFieldSet);
    }
}

