package com.it.userportrait.analy;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-04 17:21
 */
public class LeftJoin implements CoGroupFunction<String,String, JSONObject> {

    @Override
    public void coGroup(Iterable<String> iterable, Iterable<String> iterable1, Collector<JSONObject> collector) throws Exception {
        Iterator<String> adviserIter = iterable.iterator();
        Iterator<String> orderinfoIter = iterable1.iterator();
        List<String> adviserList = new ArrayList<String>();
        List<String> orderinfoList = new ArrayList<String>();
        while(adviserIter.hasNext()){
            String adviserString = adviserIter.next();
            adviserList.add(adviserString);
        }
        while(orderinfoIter.hasNext()){
            String orderinfoString = orderinfoIter.next();
            orderinfoList.add(orderinfoString);
        }
        for(String adviser:adviserList ){
            if(orderinfoList.size()>0){
                for(String orderInfoinnser:orderinfoList){
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("adviser",adviser);
                    jsonObject.put("orderinfo",orderInfoinnser);
                    collector.collect(jsonObject);
                }
            }else {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("adviser",adviser);
                jsonObject.put("orderinfo","");
                collector.collect(jsonObject);
            }
        }
    }
}

