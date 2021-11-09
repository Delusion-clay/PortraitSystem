package com.it.userportrait.reduce;

import com.it.userportrait.analy.Point;
import com.it.userportrait.analy.UserGroupEntity;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-21 16:35
 */
public class UserGroupFenBuReduce implements GroupReduceFunction<UserGroupEntity, UserGroupEntity> {
    @Override
    public void reduce(Iterable<UserGroupEntity> iterable, Collector<UserGroupEntity> collector) throws Exception {
        Iterator<UserGroupEntity> iterator = iterable.iterator();
        List<UserGroupEntity> finalList = new ArrayList<UserGroupEntity>();

        Point point = null;
        String groupField = null;
        long numbersTotal = 0;
        long timeinfo = 0;
        while(iterator.hasNext()){
            UserGroupEntity userGroupEntity = iterator.next();
            point = userGroupEntity.getCenterPoint();
            long numbers = userGroupEntity.getNumbers();
            numbersTotal += numbers;
            groupField = userGroupEntity.getGroupField();
            timeinfo = userGroupEntity.getTimeinfo();
        }
        UserGroupEntity userGroupEntityFinal = new UserGroupEntity();
        userGroupEntityFinal.setNumbers(numbersTotal);
        userGroupEntityFinal.setCenterPoint(point);
        userGroupEntityFinal.setGroupField(groupField);
        userGroupEntityFinal.setTimeinfo(timeinfo);
    }
}
