package com.it.userportrait.controller;

import com.it.userportrait.entity.BardataEntity;
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
public class UserGroupControl {
    static Map<String,String> dictmap = new HashMap<String,String>();
    static {
        dictmap.put("1","小孩");
        dictmap.put("2","快递员");
        dictmap.put("3","青年");
        dictmap.put("4","中年");
        dictmap.put("5","老人");
        dictmap.put("6","高管");
    }

    //最近一个月的用户群体分布
    @RequestMapping(value = "userGroup",method = RequestMethod.POST)
    public BardataEntity barData(){
        String sql = "select pointid,sum(numbers) as nubmers from groupFenbuinfo where timeinfoString = '201903' group by pointid ";
        List<String> xList = new ArrayList<String>();
        List<Long> yList = new ArrayList<Long>();

        try {
            ResultSet resultSet = ClickHouseUtils.getQueryResult("test",sql);
            while(resultSet.next()){
                String pointid = resultSet.getString("pointid");
                String groupname = dictmap.get(pointid);
                long numbers = resultSet.getLong("nubmers");
                xList.add(groupname);
                yList.add(numbers);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        BardataEntity bardataEntity = new BardataEntity();
        bardataEntity.setXdata(xList);
        bardataEntity.setYdata(yList);
        return  bardataEntity;
    }
}
