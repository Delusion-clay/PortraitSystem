package com.it.userportrait.map;

import com.it.userportrait.analy.UserGroupEntity;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.ReadProperties;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;

public class UserGroupFeatureMap implements MapFunction<UserGroupEntity, UserGroupEntity> {

    @Override
    public UserGroupEntity map(UserGroupEntity userGroupEntity) throws Exception {
        long userid = userGroupEntity.getUserid();
        List<UserGroupEntity> userGroupEntityList = userGroupEntity.getList();
        Collections.sort(userGroupEntityList, new Comparator<UserGroupEntity>() {
            @Override
            public int compare(UserGroupEntity o1, UserGroupEntity o2) {
                Date time1 = o1.getOrdertime();
                Date time2 = o2.getOrdertime();
                return time1.compareTo(time2);
            }
        });
        //平均消费金额、消费最大金额、消费频次
        double totalAmount = 0.0d;
        double maxAmount = Double.MIN_VALUE;
        Date preTime = null;
        Map<Integer,Integer> daysMap = new HashMap<Integer, Integer>();
        Map<String,Long> consumerFlagMap = new HashMap<String,Long>();
        Map<String,Long> timeFlagMap = new HashMap<String,Long>();
        for(UserGroupEntity userGroupEntity1:userGroupEntityList){
                double amount = userGroupEntity1.getAmount();
                Date orderTime = userGroupEntity1.getOrdertime();
            String datehour = DateUtils.getHourByTime(orderTime);
            long datahourlong = Long.valueOf(datehour);
            String timeFlag = "-1";
            if(datahourlong>=0 && datahourlong<=6){
                timeFlag = "1";
            }else if(datahourlong>=7 && datahourlong<=12){
                timeFlag = "2";
            }else if(datahourlong>=13 && datahourlong<=19){
                timeFlag = "3";
            }else if(datahourlong>=20 && datahourlong<=24){
                timeFlag = "4";
            }
            Long preLong = timeFlagMap.get(timeFlag);
            preLong = preLong==null?0l:preLong;
            timeFlagMap.put(timeFlag, preLong+1);
                String productTypId = userGroupEntity1.getProductTypeId();
                //电子 1,2,5,6   生活 4，17，56  户外 9，7，8
                String consumerFlag = ReadProperties.getKey(productTypId);
               Long preflag = consumerFlagMap.get(consumerFlag);
               preflag = preflag==null?0l:preflag;
                consumerFlagMap.put(consumerFlag,preflag+1);
                if(preTime !=null){
                    int days = DateUtils.getDaysBetweenbyStartAndend(preTime,orderTime);
                    Integer pre = daysMap.get(days);
                    pre = pre==null?0:pre;
                    daysMap.put(days,pre+1);
                }
                preTime = orderTime;

                totalAmount+=amount;
                if(amount > maxAmount){
                    maxAmount = amount;
                }
        }
        double avgAmount = totalAmount/userGroupEntityList.size();

        Set<Map.Entry<Integer,Integer>> dataSet = daysMap.entrySet();
        int totaldays = 0;
        int totalnums = 0;
        for(Map.Entry<Integer,Integer> map :dataSet){
                int days = map.getKey();
                int nums = map.getValue();
                totaldays = days*nums;
                totalnums = totalnums+nums;
        }
        int avgdays = totaldays/totalnums;//平均下单频率
        long dianZiNums = consumerFlagMap.get(0);//电子
        long shenghuoNums = consumerFlagMap.get(1);//生活
        long huwaiNums = consumerFlagMap.get(2);//户外
        long time1 = timeFlagMap.get(0);
        long time2 = timeFlagMap.get(1);
        long time3 = timeFlagMap.get(2);
        long time4 = timeFlagMap.get(3);
        userGroupEntity.setDianZiNums(dianZiNums);
        userGroupEntity.setShenghuoNums(shenghuoNums);
        userGroupEntity.setHuwaiNums(huwaiNums);
        userGroupEntity.setAvgdays(avgdays);
        userGroupEntity.setMaxAmount(maxAmount);
        userGroupEntity.setAvgAmount(avgAmount);
        userGroupEntity.setTime1(time1);
        userGroupEntity.setTime2(time2);
        userGroupEntity.setTime3(time3);
        userGroupEntity.setTime4(time4);
        String groupField = "feature=="+userid;
        userGroupEntity.setGroupField(groupField);
        return userGroupEntity;
    }
}
