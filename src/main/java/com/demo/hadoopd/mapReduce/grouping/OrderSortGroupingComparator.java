package com.demo.hadoopd.mapReduce.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/17
 * @Description
 */
public class OrderSortGroupingComparator extends WritableComparator{

    public OrderSortGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aOrderBean = (OrderBean) a;
        OrderBean bOrderBean = (OrderBean) b;

        int result;

        if(aOrderBean.getOrderId()>bOrderBean.getOrderId()){
            result = 1;
        }else if(aOrderBean.getOrderId()<bOrderBean.getOrderId()){
            result = -1;
        }else{
            result = 0;
        }

        return result;
    }
}
