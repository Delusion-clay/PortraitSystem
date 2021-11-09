package com.it.userportrait.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-04 17:27
 */
public class EntitiyTranfer {
    public static Map<String,String> tableEntityMap = new HashMap<String,String>();

    static {
        tableEntityMap.put("userinfo","com.it.userportrait.bean.UserInfo");
        tableEntityMap.put("order","com.it.userportrait.bean.Order");
        tableEntityMap.put("product","com.youfan.entity.Product");
        tableEntityMap.put("producttype","com.it.userportrait.bean.ProductType");
    }

    public static void transferAndInsert(String tableName,String data) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(data);
        String className = tableEntityMap.get(tableName);
        Class classez = Class.forName(className);
        Field[] fields = classez.getDeclaredFields();
        String id = jsonObject.getString("id");
        Map<String,String> datamap = new HashMap<String,String>();
        for(Field field :fields){
            String fieldName = field.getName();
            String valueString = jsonObject.getString(fieldName);
            if(StringUtils.isNotBlank(valueString)){
                System.out.println(fieldName);
                System.out.println(valueString);
                datamap.put(fieldName,valueString);
            }
        }
        HbaseUtils2.put(tableName,id,"info",datamap);
    }
    public static void transferAndInsertByDataMap(String tableName,String id,Map<String,String> datamap) throws Exception {
        HbaseUtils2.put(tableName,id,"info",datamap);
    }
}
