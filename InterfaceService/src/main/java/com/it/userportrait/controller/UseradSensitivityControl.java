package com.it.userportrait.controller;

import com.it.userportrait.entity.CurveEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.sql.ResultSet;
import java.text.ParseException;
import java.util.*;

@RestController
@RequestMapping("api")
@CrossOrigin
public class UseradSensitivityControl {

    static Map<String,String> dictmap = new HashMap<String,String>();
    static {
        dictmap.put("1","李宁运动鞋广告");
    }

    @RequestMapping(value = "adSensitivitData",method = RequestMethod.POST)
    public CurveEntity curveData(){
        //最近一小时
        String sql = "select advisterId,sensitivityFlag,timeinfoString,count(1) as numbers from useradSensitivityinfo group by advisterId,sensitivityFlag,timeinfoString";
        Set<String> dateSet = new HashSet<String>();
        Map<String,Map<String,Long>> dataMap = new HashMap<String,Map<String,Long>>();
        try {
            ResultSet resultSet = ClickHouseUtils.getQueryResult("test",sql);
            while(resultSet.next()){
                String advisterId = resultSet.getString("advisterId");
                String advistername = dictmap.get(advisterId);
                String sensitivityFlag = resultSet.getString("sensitivityFlag");
                String key = advistername+"=="+sensitivityFlag;
                String timeinfoString = resultSet.getString("timeinfoString");
                long numbers = resultSet.getLong("numbers");
                dateSet.add(timeinfoString);
                Map<String,Long> datainnerMap = dataMap.get(key);
                datainnerMap = datainnerMap==null?new HashMap<String,Long>():datainnerMap;

                Long data =datainnerMap.get(timeinfoString)==null?0l:datainnerMap.get(timeinfoString);
                data = data+ numbers;
                datainnerMap.put(timeinfoString,data);
                dataMap.put(key,datainnerMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<String> sortList = new ArrayList<String>(dateSet);
        Collections.sort(sortList, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int reuslt = 0;
                try {
                    reuslt = DateUtils.compareDate(o1,o2,"yyyyMMddHH");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return reuslt;
            }
        });
        Map<String,Long> dateMaptemp = new HashMap<String,Long>();
        for(String timeinfoString:sortList){
            dateMaptemp.put(timeinfoString,0l);
        }

        Set<Map.Entry<String,Map<String,Long>>> dataMapSet = dataMap.entrySet();

        List<Map<String,Object>> datalist = new ArrayList<Map<String,Object>>();
        List<Map<String,Object>> dataMapList = new ArrayList<Map<String,Object>>();//结果

        for(Map.Entry<String,Map<String,Long>> entry:dataMapSet){
            String name = entry.getKey();
            Map<String,Long> datamap = entry.getValue();
            Set<Map.Entry<String,Long>> setinner = datamap.entrySet();
            Map<String,Long> deepCopy = new HashMap<String,Long>();
            deepCopy.putAll(dateMaptemp);
            for(Map.Entry<String,Long>  innerMap : setinner){
                String key = innerMap.getKey();
                Long value = innerMap.getValue();
                deepCopy.put(key,value);
            }
            List<Long> dataList = new ArrayList<Long>();
            for(String key :sortList){
                Long value = deepCopy.get(key);
                dataList.add(value);
            }
            Map<String,Object> maptemp = new HashMap<String,Object>();
            maptemp.put("name",name);
            maptemp.put("data",dataList);
            maptemp.put("type","line");
            maptemp.put("stack","数量");
            dataMapList.add(maptemp);
        }
        List<String> legenddataList = new ArrayList<>();
        legenddataList.addAll(dataMap.keySet());
        CurveEntity curveEntity = new CurveEntity();
        curveEntity.setxList(sortList);
        curveEntity.setDataMapList(dataMapList);
        curveEntity.setLegenddataList(legenddataList);
        return  curveEntity;
    }
}
