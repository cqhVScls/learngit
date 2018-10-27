package edu.qianfeng.anlastic.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lyd on 2018/4/9.
 */
public class PaymentTypeDimension extends BaseDimension {
    private int id;
    private String paymentType;

    public PaymentTypeDimension() {

    }

    public PaymentTypeDimension(String paymentType) {
        this.paymentType = paymentType;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.paymentType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.paymentType = dataInput.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        PaymentTypeDimension other = (PaymentTypeDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        return this.paymentType.compareTo(other.paymentType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PaymentTypeDimension that = (PaymentTypeDimension) o;

        if (id != that.id) return false;
        return paymentType != null ? paymentType.equals(that.paymentType) : that.paymentType == null;

    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (paymentType != null ? paymentType.hashCode() : 0);
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }
}
