package com.it.userportrait.controller;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.entity.CurveEntity;
import com.it.userportrait.entity.KeyValueEntity;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.HbaseUtils2;
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
public class UserLabelControl {

    @RequestMapping(value = "userlabelsreal",method = RequestMethod.POST)
    public String userlabelsreal(String userid){
        List<KeyValueEntity> list = new ArrayList<KeyValueEntity>();

        try {
            String sexinfo = HbaseUtils2.getdata("userinfo",userid,"info","sexinfo");
            KeyValueEntity keyValueEntity = new KeyValueEntity();
            keyValueEntity.setKey("性别");
            keyValueEntity.setValue(sexinfo);
            list.add(keyValueEntity);
            String brandlist = HbaseUtils2.getdata("userinfo",userid,"info","brandlist");
            keyValueEntity = new KeyValueEntity();
            keyValueEntity.setKey("品牌偏好");
            keyValueEntity.setValue(brandlist);
            list.add(keyValueEntity);
            ResultSet resultSet =  ClickHouseUtils.getQueryResult("test","select carrierString,count(1) as nubmers from carrieruserinfo where userid = "+userid+" group by carrierString");
            Map<String,String> datamap = new HashMap<String,String>();
            while(resultSet.next()) {
                String carrierString = resultSet.getString("carrierString");
                long numbers = resultSet.getLong("nubmers");
                datamap.put(carrierString,numbers+"");
            }
            keyValueEntity = new KeyValueEntity();
            keyValueEntity.setKey("品牌运营商");
            keyValueEntity.setValue(JSONObject.toJSONString(datamap));

            list.add(keyValueEntity);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String result = JSONObject.toJSONString(list);
        return  result;
    }

}
