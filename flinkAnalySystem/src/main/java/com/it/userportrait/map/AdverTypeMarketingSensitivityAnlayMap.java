package com.it.userportrait.map;

import com.it.userportrait.analy.MarketingSensitivityEntity;
import org.apache.flink.api.common.functions.MapFunction;


public class AdverTypeMarketingSensitivityAnlayMap implements MapFunction<MarketingSensitivityEntity, MarketingSensitivityEntity> {

    @Override
    public MarketingSensitivityEntity map(MarketingSensitivityEntity marketingSensitivityEntity) throws Exception {
        long timeinfo = marketingSensitivityEntity.getTimeinfo();
        int advType = marketingSensitivityEntity.getAdvType();
        long advernums = marketingSensitivityEntity.getAdvernums();
        long ordernums = marketingSensitivityEntity.getOrdernums();
        //非常敏感2或者点击5+ + 1 一般5+ 或者1+ 1  不敏感2
        String sensitivityFlag = "";
        if(advernums<=2&&ordernums==0){
            sensitivityFlag = "不敏感";
            //不敏感
        }else if((advernums>5&&advernums==0)||(advernums>1&&advernums==1)){
            sensitivityFlag = "一般";
            //一般
        }else if((advernums>1&&advernums>1)||(advernums>5&&advernums==1)){
            sensitivityFlag = "非常敏感";
            //非常敏感
        }
        MarketingSensitivityEntity marketingSensitivityEntityResult
                = new MarketingSensitivityEntity();
        String avTypeName = advType==0?"动画":
                advType==1? "纯文字":advType==2? "视屏":"文字加动画";
        String groupField = "avType=="+timeinfo+
                "=="+advType+"=="+sensitivityFlag;
        long numbers = 1l;
         marketingSensitivityEntityResult.setGroupField(groupField);
         marketingSensitivityEntityResult.setAvTypeName(avTypeName);
         marketingSensitivityEntityResult.setNumbers(numbers);
         marketingSensitivityEntityResult.setTimeinfo(timeinfo);
         marketingSensitivityEntityResult.setSensitivityFlag(sensitivityFlag);
        return marketingSensitivityEntityResult;
    }
}
