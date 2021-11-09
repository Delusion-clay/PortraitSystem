package com.it.userportrait.reduce;

import com.it.userportrait.analy.KeywordEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.List;

public class Keyword2AnlayReduce implements ReduceFunction<KeywordEntity> {


    @Override
    public KeywordEntity reduce(KeywordEntity keywordEntity, KeywordEntity t1) throws Exception {
        long count1 = keywordEntity.getTotaldocumet();
        long count2 = t1.getTotaldocumet();
        keywordEntity.setTotaldocumet(count1+count2);
        return keywordEntity;
    }
}
