package com.it.userportrait.controller;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.entity.SandianEntity;
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
public class CarrierQushiControl {


    @RequestMapping(value = "carrierQushiData",method = RequestMethod.POST)
    public String sandianData(){

        //最近一小时
        String sql = "select userid,carrierString,timeinfoString,count(1) as nubmers from carrieruserinfo group by userid,carrierString,timeinfoString";
        Set<String> dateSet = new HashSet<String>();
        Map<String,Map<String,Long>> dataMap = new HashMap<String,Map<String,Long>>();
        try {
            ResultSet resultSet = ClickHouseUtils.getQueryResult("test",sql);
            while(resultSet.next()){
                String carrierString = resultSet.getString("carrierString");
                String timeinfoString = resultSet.getString("timeinfoString");
                long numbers = resultSet.getLong("nubmers");
                dateSet.add(timeinfoString);
                Map<String,Long> datainnerMap = dataMap.get(carrierString);
                datainnerMap = datainnerMap==null?new HashMap<String,Long>():datainnerMap;

                Long data =datainnerMap.get(timeinfoString)==null?0l:datainnerMap.get(timeinfoString);
                data = data+ numbers;
                datainnerMap.put(timeinfoString,data);
                dataMap.put(carrierString,datainnerMap);
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
                    reuslt = DateUtils.compareDate(o1,o2,"yyyyMMddHHmm");
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


        List<List<Object>> listyidong = new ArrayList<List<Object>>();
        List<List<Object>> datadianxin = new ArrayList<List<Object>>();
        List<List<Object>> dataliantong = new ArrayList<List<Object>>();

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
            for(int j=0;j<sortList.size();j++){
                String datetime = sortList.get(j);
                Long value = deepCopy.get(datetime);
                List<Object> list1 = new ArrayList<Object>();
                list1.add(j+1);
                list1.add(value);
                list1.add(datetime);
                if(name.equals("移动")){
                    listyidong.add(list1);
                }else if(name.equals("电信")){
                    datadianxin.add(list1);
                }else if(name.equals("联通")){
                    dataliantong.add(list1);
                }
            }
        }

        List<String> dataList = new ArrayList<String>();
        dataList.add("移动");
        dataList.add("电信");
        dataList.add("联通");

        SandianEntity sandianEntity = new SandianEntity();

        sandianEntity.setDataList(dataList);
        sandianEntity.setList1(listyidong);
        sandianEntity.setList2(datadianxin);
        sandianEntity.setList3(dataliantong);
        String result  = JSONObject.toJSONString(sandianEntity);
        return  result;
    }
}
