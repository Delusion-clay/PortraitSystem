package com.it.userportrait.controller;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.entity.BardataEntity;
import com.it.userportrait.entity.CurveEntity;
import com.it.userportrait.entity.KeyValueEntity;
import com.it.userportrait.entity.SandianEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("api")
@CrossOrigin
public class ServiceInterfaceControl {

    @RequestMapping(value = "userlabels",method = RequestMethod.POST)
    public String userlabels(){
        List<KeyValueEntity> list = new ArrayList<KeyValueEntity>();
        KeyValueEntity keyValueEntity = new KeyValueEntity();
        keyValueEntity.setKey("羊毛党");
        keyValueEntity.setValue("是");
        list.add(keyValueEntity);
        keyValueEntity = new KeyValueEntity();
        keyValueEntity.setKey("性别");
        keyValueEntity.setValue("女");
        list.add(keyValueEntity);
        String result = JSONObject.toJSONString(list);
        return  result;
    }

    @RequestMapping(value = "curveData",method = RequestMethod.POST)
    public CurveEntity curveData(){
        List<String> xList = new ArrayList<String>();
        xList.add("20200801");
        xList.add("20200802");
        xList.add("20200803");
        xList.add("20200804");
        xList.add("20200805");
        xList.add("20200806");
        List<Map<String,Object>> dataMapList = new ArrayList<Map<String,Object>>();
        Map<String,Object> dataMap = new HashMap<String,Object>();
        dataMap.put("name","支付宝");
        dataMap.put("type","line");
        dataMap.put("stack","数量");
        List<Long> data = new ArrayList<Long>();
        data.add(100l);
        data.add(105l);
        data.add(200l);
        data.add(201l);
        data.add(156l);
        data.add(146l);
        dataMap.put("data",data);
        dataMapList.add(dataMap);


        dataMap = new HashMap<String,Object>();
        dataMap.put("name","微信支付");
        dataMap.put("type","line");
        dataMap.put("stack","数量");
        data = new ArrayList<Long>();
        data.add(400l);
        data.add(205l);
        data.add(300l);
        data.add(241l);
        data.add(267l);
        data.add(368l);
        dataMap.put("data",data);
        dataMapList.add(dataMap);
        CurveEntity curveEntity = new CurveEntity();
        curveEntity.setxList(xList);
        curveEntity.setDataMapList(dataMapList);
        return  curveEntity;
    }


    @RequestMapping(value = "sandianData",method = RequestMethod.POST)
    public String sandianData(){
        List<String> dataList = new ArrayList<String>();
        dataList.add("小米");
        dataList.add("华为");
        dataList.add("oppo");
        List<List<Object>> listxiaomi = new ArrayList<List<Object>>();
        List<Object> list1 = new ArrayList<Object>();
        list1.add(1);
        list1.add(9);
        list1.add("20200801");

        listxiaomi.add(list1);

        list1 = new ArrayList<Object>();
        list1.add(2);
        list1.add(7);
        list1.add("20200802");
        listxiaomi.add(list1);

        list1 = new ArrayList<Object>();
        list1.add(3);
        list1.add(8);
        list1.add("20200803");
        listxiaomi.add(list1);


        List<List<Object>> huaweilist = new ArrayList<List<Object>>();
        list1 = new ArrayList<Object>();
        list1.add(1);
        list1.add(20);
        list1.add("20200801");
        huaweilist.add(list1);

        list1 = new ArrayList<Object>();
        list1.add(2);
        list1.add(24);
        list1.add("20200802");
        huaweilist.add(list1);

        list1 = new ArrayList<Object>();
        list1.add(3);
        list1.add(29);
        list1.add("20200803");
        huaweilist.add(list1);


        List<List<Object>> oppolist = new ArrayList<List<Object>>();
        list1 = new ArrayList<Object>();
        list1.add(1);
        list1.add(40);
        list1.add("20200801");
        oppolist.add(list1);

        list1 = new ArrayList<Object>();
        list1.add(2);
        list1.add(44);
        list1.add("20200802");
        oppolist.add(list1);

        list1 = new ArrayList<Object>();
        list1.add(3);
        list1.add(46);
        list1.add("20200803");
        oppolist.add(list1);

        SandianEntity sandianEntity = new SandianEntity();

        sandianEntity.setDataList(dataList);
        sandianEntity.setHuaweilist(huaweilist);
        sandianEntity.setListxiaomi(listxiaomi);
        sandianEntity.setOppolist(oppolist);
        String result  = JSONObject.toJSONString(sandianEntity);
        return  result;
    }


    @RequestMapping(value = "barData",method = RequestMethod.POST)
    public BardataEntity barData(){
        BardataEntity bardataEntity = new BardataEntity();
        List<String> xList = new ArrayList<String>();
        xList.add("20200801");
        xList.add("20200802");
        xList.add("20200803");
        xList.add("20200804");

        List<Long> yList = new ArrayList<Long>();
        yList.add(10l);
        yList.add(22l);
        yList.add(27l);
        yList.add(29l);
        bardataEntity.setXdata(xList);
        bardataEntity.setYdata(yList);
        return  bardataEntity;
    }
}
