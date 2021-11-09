package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.KeywordEntity;
import com.it.userportrait.bean.Order;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

public class KeywordMap implements MapFunction<String, KeywordEntity> {

    @Override
    public KeywordEntity map(String s) throws Exception {
        Order youfanorder = JSONObject.parseObject(s, Order.class);
        long productId = youfanorder.getProductid();
        long userId = youfanorder.getUserid();
        String productTitile = HbaseUtils2.getdata("product",productId+"","info","productTitile");
        KeywordEntity keywordEntity = new KeywordEntity();
        keywordEntity.setUserid(userId);
        ArrayList<String> title  = new ArrayList<String>();
        title.add(productTitile);
        String groupField = "Keword=="+userId;
        keywordEntity.setGroupField(groupField);
        keywordEntity.setProductTitile(title);
        return keywordEntity;
    }
}
