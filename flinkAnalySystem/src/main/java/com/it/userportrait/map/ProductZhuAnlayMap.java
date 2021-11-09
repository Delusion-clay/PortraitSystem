package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.ProductZhuEntity;
import com.it.userportrait.bean.Order;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.ReadProperties;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class ProductZhuAnlayMap implements MapFunction<String, ProductZhuEntity> {

    @Override
    public ProductZhuEntity map(String s) throws Exception {
        Order youfanorder = JSONObject.parseObject(s, Order.class);
        long userid = youfanorder.getUserid();
        Date date =  youfanorder.getCreateTime();
        long productid = youfanorder.getProductid();
        int payStatus = youfanorder.getPaystatus();
        if(payStatus == 1){
            ProductZhuEntity productZhuEntity = new ProductZhuEntity();
            productZhuEntity.setUserid(userid);
            long timeinfo = DateUtils.getCurrentDayStart(date.getTime());
            productZhuEntity.setTimeinfo(timeinfo);
            productZhuEntity.setNumbers(1l);
            String productzhu = ReadProperties.getKey(productid+"");
            productzhu = productzhu==null?"-1" : productzhu;
            productZhuEntity.setProductZhuFlag(productzhu);
            return productZhuEntity;
        }
        return null;
    }
}
