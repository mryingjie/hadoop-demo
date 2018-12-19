package com.demo.hadoopd.mapreduce.grouping;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author ZhengYingjie
 * @Date 2018/12/17
 * @Description
 */
public class OrderBean implements WritableComparable<OrderBean> {
    /**
     * 订单id
     */
    private Integer orderId;

    /**
     * 价格
     */
    private Double price;

    public OrderBean() {
    }

    public OrderBean(Integer orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int result;
        if(this.orderId > o.getOrderId()) {
            result = 1;
        } else if(this.orderId < o.getOrderId()) {
            result = -1;
        }else{
            result = this.price > o.getPrice()? -1 : 1;
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.orderId);
        dataOutput.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId=dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return  orderId +
                "\t" + price ;
    }
}
