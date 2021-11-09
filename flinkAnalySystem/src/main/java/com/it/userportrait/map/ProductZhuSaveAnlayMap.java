package com.it.userportrait.map;

import com.alibaba.fastjson.JSONObject;
import com.it.userportrait.analy.ProductZhuEntity;
import com.it.userportrait.util.DateUtils;
import com.it.userportrait.util.HbaseUtils2;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Date;

public class ProductZhuSaveAnlayMap implements MapFunction<ProductZhuEntity, ProductZhuEntity> {


    @Override
    public ProductZhuEntity map(ProductZhuEntity productZhuEntity) throws Exception {
        long userid = productZhuEntity.getUserid();
        long numbers = productZhuEntity.getNumbers();
        String productZhuFlag = productZhuEntity.getProductZhuFlag();
        long timeinfoday = productZhuEntity.getTimeinfo();
        long timeinfomonth = DateUtils.getCurrentMonthStart(timeinfoday);
        String tablename = "procutZhuuserinfo";
        String rowkey = userid+"=="+timeinfomonth;
        String familyName = "info";
        String coulumprocutZhunums = productZhuFlag;
        String preString = HbaseUtils2.getdata(tablename,rowkey,familyName,coulumprocutZhunums);
        long pre = StringUtils.isBlank(preString)?0:Long.valueOf(preString);
        if(pre<3){
                if(numbers+pre>2){
                    HbaseUtils2.putdata("userinfo",userid+"","info",productZhuFlag,"1");
                    ProductZhuEntity productZhuEntityresult = new ProductZhuEntity();
                    productZhuEntityresult.setTimeinfo(timeinfoday);
                    productZhuEntityresult.setNumbers(1);
                    productZhuEntityresult.setProductZhuFlag(productZhuFlag);
                    String groupfiled = "ProductZhuSave=="+timeinfoday+"=="+productZhuFlag;
                    productZhuEntityresult.setGroupField(groupfiled);
                    return productZhuEntityresult;
                }
        }
        HbaseUtils2.putdata(tablename,rowkey,familyName,coulumprocutZhunums,(numbers+pre)+"");

        return null;
    }
}
