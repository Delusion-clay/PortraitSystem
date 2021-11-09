package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.BrandEntity;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-04 18:19
 */
public class UserBrandSaveMap implements MapFunction<BrandEntity,BrandEntity> {
    @Override
    public BrandEntity map(BrandEntity brandEntity) throws Exception {
        long userid = brandEntity.getUserid();
        String brand = brandEntity.getBrand();
        long nubmers = brandEntity.getNumbers();
        String timeinfo = brandEntity.getTimeinfo();
        String tablename = "userinfo";
        String rowkey = userid+"";
        String famliyname="info";
        String colum = "brandlist";

        String brandlistliststring = HbaseUtils2.getdata(tablename,rowkey,famliyname,colum);
        List<Map> result = new ArrayList<Map>();
        if( StringUtils.isNotBlank(brandlistliststring)){
            result = JSONObject.parseArray(brandlistliststring,Map.class);
        }
        List<Map> newresult = new ArrayList<Map>();
        for(Map map :result){
            String brandinner = map.get("key").toString();
            Long value = Long.valueOf(map.get("value").toString());
            if(brand.equals(brandinner)){
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
        BrandEntity brandEntityresult = new BrandEntity();
        String groupField = "brandBy=="+brand+"=="+timeinfo;
        brandEntityresult.setGroupField(groupField);
        brandEntityresult.setBrand(brand);
        brandEntityresult.setTimeinfo(timeinfo);
        brandEntityresult.setNumbers(1l);
        return brandEntityresult;
    }
}
