package com.ls.mapreduce.orderCount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ItemBean implements WritableComparable<ItemBean> {
    private Text itemId;
    private DoubleWritable amount;

    public ItemBean(Text itemId, DoubleWritable amount) {
        this.itemId = itemId;
        this.amount = amount;
    }


    public ItemBean() {
    }


    public Text getItemId() {
        return itemId;
    }

    public void setItemId(Text itemId) {
        this.itemId = itemId;
    }

    public DoubleWritable getAmount() {
        return amount;
    }

    public void setAmount(DoubleWritable amount) {
        this.amount = amount;
    }
    public void set(Text itemId, DoubleWritable amount) {
        this.itemId = itemId;
        this.amount = amount;
    }

    public int compareTo(ItemBean o) {
        int ret = this.itemId.compareTo(o.getItemId());
        if(ret == 0){
            return this.amount.compareTo(o.getAmount());
        }

        return ret;
    }

    public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(itemId.toString());
            dataOutput.writeDouble(amount.get());
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.itemId = new Text(dataInput.readUTF());
        this.amount = new DoubleWritable(dataInput.readDouble());
    }

    @Override
    public String toString() {
        return "ItemBean{" +
                "itemId=" + itemId +
                ", amount=" + amount +
                '}';
    }
}
