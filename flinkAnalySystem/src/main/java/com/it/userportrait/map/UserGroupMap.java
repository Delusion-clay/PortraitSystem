package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.UserGroupEntity;
import com.it.userportrait.bean.Order;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class UserGroupMap implements MapFunction<String, UserGroupEntity> {
    public UserGroupEntity map(String s) throws Exception {
        Order youfanorder = JSONObject.parseObject(s, Order.class);
        Date ordertime = youfanorder.getCreateTime();
        long userid = youfanorder.getUserid();
        double amount = youfanorder.getAmount();
        long productid = youfanorder.getProductid();
         int payStatus = youfanorder.getPaystatus();
         if(payStatus == 1){
             UserGroupEntity  userGroupEntity = new  UserGroupEntity();
             userGroupEntity.setUserid(userid);
             userGroupEntity.setOrdertime(ordertime);
             userGroupEntity.setAmount(amount);
             String tablename = "product ";
             String rowkey = productid+"";
             String famlyName = "info";
             String cloumn = "productTypeid";
             String productTypeId = HbaseUtils2.getdata(tablename,rowkey,famlyName,cloumn);
             userGroupEntity.setProductTypeId(productTypeId);
             userGroupEntity.setGroupField("usergroupmap=="+userid);
             List<UserGroupEntity> list = new ArrayList<UserGroupEntity>();
             list.add(userGroupEntity);
             userGroupEntity.setList(list);
             return userGroupEntity;
         }

         return null;
    }
}
