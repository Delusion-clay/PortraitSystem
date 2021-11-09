package com.it.userportrait.reduce;

import com.it.userportrait.analy.UserGroupEntity;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-21 16:29
 */
public class UserGroupReduce implements GroupReduceFunction<UserGroupEntity, UserGroupEntity> {
    @Override
    public void reduce(Iterable<UserGroupEntity> iterable, Collector<UserGroupEntity> collector) throws Exception {
        Iterator<UserGroupEntity> iterator = iterable.iterator();
        List<UserGroupEntity> finalList = new ArrayList<UserGroupEntity>();
        UserGroupEntity userGroupEntityFinal = null;
        while(iterator.hasNext()){
            UserGroupEntity userGroupEntity = iterator.next();
            List<UserGroupEntity> userGroupEntityList = userGroupEntity.getList();
            finalList.addAll(userGroupEntityList);
            userGroupEntityFinal = userGroupEntity;
        }
        if(userGroupEntityFinal != null){
            userGroupEntityFinal.setList(finalList);
            collector.collect(userGroupEntityFinal);
        }
    }
}
