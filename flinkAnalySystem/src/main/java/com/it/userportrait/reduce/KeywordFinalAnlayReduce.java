package com.it.userportrait.reduce;

import com.it.userportrait.analy.KeywordEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.List;

public class KeywordFinalAnlayReduce implements ReduceFunction<KeywordEntity> {


    @Override
    public KeywordEntity reduce(KeywordEntity keywordEntity, KeywordEntity t1) throws Exception {
        KeywordEntity keywordEntityresult = new KeywordEntity();
        keywordEntityresult.setTimeinfoString(keywordEntity.getTimeinfoString());
        keywordEntityresult.setNumbers(keywordEntity.getNumbers()+t1.getNumbers());
        keywordEntityresult.setGroupField(keywordEntity.getGroupField());
        String top1 = keywordEntity.getTop1();
        String top2 = keywordEntity.getTop2();
        String top3 = keywordEntity.getTop3();
        keywordEntityresult.setTop1(top1);
        keywordEntityresult.setTop2(top2);
        keywordEntityresult.setTop3(top3);
        return keywordEntityresult;
    }
}
