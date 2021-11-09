package com.it.userportrait.reduce;

import com.it.userportrait.analy.MemberEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

public class MemberAnlayReduce implements ReduceFunction<MemberEntity> {

    public MemberEntity reduce(MemberEntity memberEntity,MemberEntity t1) throws Exception {
        long numbers1 = 0l;
        String groupField = "";
        if(memberEntity != null){
            numbers1 =memberEntity.getNumbers();
            groupField = memberEntity.getGroupField();
        }
        long numbers2 = 0l;
        if(t1 != null){
            numbers2 = t1.getNumbers();
            groupField = t1.getGroupField();
        }

        if(StringUtils.isNotBlank(groupField)){
            MemberEntity memberEntity1 = new MemberEntity();
            memberEntity1.setGroupField(groupField);
            memberEntity1.setNumbers(numbers1+numbers2);
            return  memberEntity1;
        }
        return null;
    }
}
