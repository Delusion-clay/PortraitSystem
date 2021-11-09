package com.it.userportrait.reduce;

import com.it.userportrait.analy.ProductZhuEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ProductZhuSaveAnalyReduce implements ReduceFunction<ProductZhuEntity> {


    @Override
    public ProductZhuEntity reduce(ProductZhuEntity productZhuEntity, ProductZhuEntity t1) throws Exception {
        ProductZhuEntity fianlResulot = null;
        if(productZhuEntity !=null ){
            fianlResulot = productZhuEntity;
        }
        if(t1 != null){
            fianlResulot = productZhuEntity;
        }
        if(fianlResulot != null){
            long nubmer1 = productZhuEntity == null?0:productZhuEntity.getNumbers();
            long nubmer2 = t1 == null?0:t1.getNumbers();
            fianlResulot.setNumbers(nubmer1+nubmer2);
            return fianlResulot;
        }
        return null;
    }
}
