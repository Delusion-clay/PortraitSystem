package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.ProducttypeEntity;
import com.it.userportrait.log.ScanOpertor;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class ProductTypeMap implements MapFunction<String, ProducttypeEntity> {


    @Override
    public ProducttypeEntity  map(String s) throws Exception {
        ScanOpertor scanOpertor = JSONObject.parseObject(s, ScanOpertor.class);
        long userid = scanOpertor.getUserid();
        String productTypeid = scanOpertor.getProductTypeId()+"";
        String tablename = "userinfo";
        String rowkey = userid+"";
        String famliyname="info";
        String colum = "producttypelist";
        String producttypeliststring = HbaseUtils2.getdata(tablename,rowkey,famliyname,colum);
        List<Map> result = new ArrayList<Map>();
        if( StringUtils.isNotBlank(producttypeliststring)){
            result = JSONObject.parseArray(producttypeliststring,Map.class);
        }
        List<Map> newresult = new ArrayList<Map>();
        for(Map map :result){
                String prtypeid = map.get("key").toString();
                Long value = Long.valueOf(map.get("value").toString());
                if(productTypeid.equals(prtypeid)){
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

        ProducttypeEntity producttypeEntity = new ProducttypeEntity();
        //productType==timehour==productTypeid
        String groupFiled = "productType=="+ DateUtils.getCurrentHourStart(System.currentTimeMillis())+"=="+productTypeid;
        producttypeEntity.setUserid(userid+"");
        producttypeEntity.setProducttypeid(productTypeid);
        producttypeEntity.setGroupField(groupFiled);
        producttypeEntity.setNumbers(1l);
        return producttypeEntity;
    }
}
