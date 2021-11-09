package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.ConsumeEntity;
import com.it.userportrait.bean.Order;
import com.it.userportrait.util.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Map;

public class ConsumeAnlayMap implements MapFunction<String, ConsumeEntity> {

    /**
     * 高消费   >1000 订单金额 4次  或者>200 次数15 次每月 >1000  1  > 200  2 < 200 3
     * 中等消费  >200 次数<15 次每月 >1000 小于4次
     * 低消费  <200 次数<15
     * @param s
     * @return
     * @throws Exception
     */
    @Override
    public ConsumeEntity map(String s) throws Exception {
        Order order = JSONObject.parseObject(s, Order.class);
        Double amount = order.getAmount();
        long userid = order.getUserid();
        String consumeFlag = "";
        if(amount>1000){
            consumeFlag = "1";
        }else if(amount> 200){
            consumeFlag = "2";
        }else if(amount < 200){
            consumeFlag = "3";
        }
        ConsumeEntity consumeEntity = new ConsumeEntity();
        consumeEntity.setConsumeFlag(consumeFlag);
        consumeEntity.setUserid(userid);
        long timeinfo = DateUtils.getCurrentMonthStart(System.currentTimeMillis());
        String groupField = "consume=="+userid+"=="+timeinfo+"=="+consumeFlag;
        consumeEntity.setGroupField(groupField);
        String timeinfoString = DateUtils.getDateString(timeinfo,"yyyyMM");
        consumeEntity.setTimeinfoString(timeinfoString);
        return consumeEntity;
    }
}
