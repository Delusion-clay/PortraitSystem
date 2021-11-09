package com.it.userportrait.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ReadProperties {
    public final static Config config = ConfigFactory.load("prodcutCat.properties");
    public static String getKey(String key){
        return config.getString(key).trim();
    }
    public static String getKey(String key,String filename){
        Config config =  ConfigFactory.load(filename);
        return config.getString(key).trim();
    }
}
