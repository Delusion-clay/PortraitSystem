package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.EntanglementProductEntity;
import com.it.userportrait.log.CartOpertor;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class EntanglementProductMap implements MapFunction<String, EntanglementProductEntity> {


    @Override
    public  EntanglementProductEntity  map(String s) throws Exception {
        CartOpertor cartOpertor = JSONObject.parseObject(s, CartOpertor.class);
        long userid = cartOpertor.getUserid();
        String productId = cartOpertor.getProductId()+"";
        String tablename = "userinfo";
        String rowkey = userid+"";
        String famliyname="info";
        String colum = "entanglementProducts";
        String entanglementProducts = HbaseUtils2.getdata(tablename,rowkey,famliyname,colum);
        List<Map> result = new ArrayList<Map>();
        if( StringUtils.isNotBlank(entanglementProducts)){
            result = JSONObject.parseArray(entanglementProducts,Map.class);
        }
        List<Map> newresult = new ArrayList<Map>();
        for(Map map :result){
                String proId = map.get("key").toString();
                Long value = Long.valueOf(map.get("value").toString());
                if(productId.equals(proId)){
                        value = value+1;
                        map.put("value",value);
                }
                newresult.add(map);
        }
        Collections.sort(newresult, new Comparator<Map>() {
            @Override
            public int compare(Map o1, Map o2) {
                Long value1 = Long.valueOf(o1.get("value").toString());
                Long value2 = Long.valueOf(o2.get("value").toString());
                return value2.compareTo(value1);
            }
        });
        if(newresult.size() > 5){
          newresult = newresult.subList(0,5);
        }
        String data = JSONObject.toJSONString(newresult);
        HbaseUtils2.putdata(tablename,rowkey,famliyname,colum,data);

        EntanglementProductEntity entanglementProductEntity = new EntanglementProductEntity();
        String groupFiled = "/entanglemntProduct=="+ DateUtils.getCurrentHourStart(System.currentTimeMillis())+"=="+productId;
        entanglementProductEntity.setUserid(userid+"");
        entanglementProductEntity.setProductid(productId);
        entanglementProductEntity.setGroupField(groupFiled);
        entanglementProductEntity.setNumbers(1l);
        return entanglementProductEntity;
    }
}
