package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.MemberEntity;
import com.it.userportrait.bean.UserInfo;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MemberAnlayMap implements MapFunction<String, MemberEntity> {
    public MemberEntity map(String s) throws Exception {
        Map<String,String> datamap = JSONObject.parseObject(s, Map.class);
        String typecurent = datamap.get("typecurent");
        MemberEntity memberEntity = new MemberEntity();
        if(typecurent.equals("INSERT")){
            UserInfo userInfo = JSONObject.parseObject(s, UserInfo.class);
            int status = userInfo.getStatus();
            String memberFlag = status == 0?"普通会员 ":status == 1?"白银会员":"黄金会员";
            Map<String,String> mapdata = new HashMap<String,String>();
            String tableName = "memberinfo";
            mapdata.put("userid",userInfo.getId()+"");
            mapdata.put("memberFlag",memberFlag);
            String fiveMinunite = DateUtils.getByinterMinute(System.currentTimeMillis()+"");
            mapdata.put("timeinfoString",fiveMinunite);
            Set<String> intFieldSet = new HashSet<String>();
            intFieldSet.add("userid");
            ClickHouseUtils.insertData(tableName,mapdata,intFieldSet);
            String groupField = "memberFlag=="+fiveMinunite+"=="+memberFlag;
            long numbers = 1l;
            memberEntity.setGroupField(groupField);
            memberEntity.setNumbers(numbers);
        }
        return memberEntity;
    }
}
