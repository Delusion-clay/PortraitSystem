package com.it.userportrait.reduce;

import com.it.userportrait.analy.ProducttypeEntity;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ProductTypeAnlayReduce implements ReduceFunction<ProducttypeEntity> {

    public ProducttypeEntity reduce(ProducttypeEntity producttypeEntity, ProducttypeEntity t1) throws Exception {
        long numbers1 = 0l;
        String groupField = "";
        if(producttypeEntity != null){
            numbers1 =producttypeEntity.getNumbers();
            groupField = producttypeEntity.getGroupField();
        }
        long numbers2 = 0l;
        if(t1 != null){
            numbers2 = t1.getNumbers();
            groupField = t1.getGroupField();
        }

        if(StringUtils.isNotBlank(groupField)){
            ProducttypeEntity producttypeEntity1 = new ProducttypeEntity();
            producttypeEntity1.setGroupField(groupField);
            producttypeEntity1.setNumbers(numbers1+numbers2);
            return  producttypeEntity1;
        }
        return null;
    }
}
