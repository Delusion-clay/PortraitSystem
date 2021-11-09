package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.CarrierEntity;
import com.it.userportrait.bean.UserInfo;
import com.it.userportrait.util.CarrierUtils;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CarrierAnlayMap implements MapFunction<String, CarrierEntity> {
    public CarrierEntity map(String s) throws Exception {
        Map<String,String> datamap = JSONObject.parseObject(s, Map.class);
        String typecurent = datamap.get("typecurent");
        CarrierEntity carrierEntity = new CarrierEntity();
        if(typecurent.equals("INSERT")){
            UserInfo userInfo = JSONObject.parseObject(s, UserInfo.class);
            String telphone = userInfo.getPhone();
            int carrier = CarrierUtils.getCarrierByTel(telphone);
            String carrierString = carrier == 0?"未知":carrier == 1?"移动":carrier == 2?"联通":"电信";

            String tableName = "carrieruserinfo";
            Map<String,String> mapdata = new HashMap<String,String>();
            mapdata.put("userid",userInfo.getId()+"");
            mapdata.put("carrierString",carrierString);
            String fiveMinunite = DateUtils.getByinterMinute(System.currentTimeMillis()+"");
            mapdata.put("timeinfoString",fiveMinunite);
            Set<String> intFieldSet = new HashSet<String>();
            ClickHouseUtils.insertData(tableName,mapdata,intFieldSet);
            String groupField = "carrierString=="+fiveMinunite+"=="+carrierString;
            long numbers = 1l;
            carrierEntity.setGroupField(groupField);
            carrierEntity.setNumbers(numbers);
        }
        return carrierEntity;
    }
}

