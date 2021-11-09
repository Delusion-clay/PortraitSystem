package com.it.userportrait.map;
import com.it.userportrait.analy.YearsEntity;
import com.it.userportrait.bean.UserInfo;
import com.it.userportrait.util.ClickHouseUtils;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.YearsUtils;
import org.apache.flink.api.common.functions.MapFunction;
import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
/**
 * @description:
 * @author: Delusion
 * @date: 2021-07-20 11:15
 */
public class YearsAnlayMap implements MapFunction<String, YearsEntity> {
    public YearsEntity map(String s) throws Exception {
        Map<String,String> datamap = JSONObject.parseObject(s, Map.class);
        String typecurent = datamap.get("typecurent");
        YearsEntity yearsEntity = new YearsEntity();
        if(typecurent.equals("INSERT")){
            UserInfo userInfo = JSONObject.parseObject(s, UserInfo.class);
            String yearlabel = YearsUtils.getYears(userInfo.getAge());
            String tableName = "yearsuserinfo";
            Map<String,String> mapdata = new HashMap<String,String>();
            mapdata.put("userid",userInfo.getId()+"");
            mapdata.put("yearslabel",yearlabel);
            String fiveMinunite = DateUtils.getByinterMinute(System.currentTimeMillis()+"");
            mapdata.put("timeinfoString",fiveMinunite);
            Set<String> intFieldSet = new HashSet<String>();
            ClickHouseUtils.insertData(tableName,mapdata,intFieldSet);
            String groupField = "yearslabel=="+fiveMinunite+"=="+yearlabel;
            long numbers = 1l;
            yearsEntity.setGroupField(groupField);
            yearsEntity.setNumbers(numbers);
        }
        return yearsEntity;
    }
}

