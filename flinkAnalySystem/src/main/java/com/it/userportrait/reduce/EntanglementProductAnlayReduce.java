package com.it.userportrait.reduce;


import com.it.userportrait.analy.EntanglementProductEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

public class EntanglementProductAnlayReduce implements ReduceFunction<EntanglementProductEntity> {

    public EntanglementProductEntity reduce(EntanglementProductEntity entanglementProductEntity, EntanglementProductEntity t1) throws Exception {
        long numbers1 = 0l;
        String groupField = "";
        if(entanglementProductEntity != null){
            numbers1 =entanglementProductEntity.getNumbers();
            groupField = entanglementProductEntity.getGroupField();
        }
        long numbers2 = 0l;
        if(t1 != null){
            numbers2 = t1.getNumbers();
            groupField = t1.getGroupField();
        }

        if(StringUtils.isNotBlank(groupField)){
            EntanglementProductEntity entanglementProductEntity1 = new EntanglementProductEntity();
            entanglementProductEntity1.setGroupField(groupField);
            entanglementProductEntity1.setNumbers(numbers1+numbers2);
            return  entanglementProductEntity1;
        }
        return null;
    }
}
