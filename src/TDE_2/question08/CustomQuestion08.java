package TDE_2.question08;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CustomQuestion08 implements Writable {

    private String country;
    private String year;
    private double minAmount;
    private double maxAmount;

    public CustomQuestion08() {
    }

    public CustomQuestion08(String country, String year, double minAmount, double maxAmount) {
        this.country = country;
        this.year = year;
        this.minAmount = minAmount;
        this.maxAmount = maxAmount;
    }

    public String getCountry() {
        return country;
    }

    public String getYear() {
        return year;
    }

    public double getMinAmount() {
        return minAmount;
    }

    public double getMaxAmount() {
        return maxAmount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(country);
        out.writeUTF(year);
        out.writeDouble(minAmount);
        out.writeDouble(maxAmount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country = in.readUTF();
        year = in.readUTF();
        minAmount = in.readDouble();
        maxAmount = in.readDouble();
    }

    @Override
    public String toString() {
        return country + "\t" + year + "\tMIN: " + minAmount + "\tMAX: " + maxAmount;
    }
}
