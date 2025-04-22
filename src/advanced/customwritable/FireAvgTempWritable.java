package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FireAvgTempWritable implements Writable{

    private int n;
    private float temperature;


    public FireAvgTempWritable() {}

    public FireAvgTempWritable(int n, float temperature) {
        this.n = n;
        this.temperature = temperature;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(n));
        dataOutput.writeUTF(String.valueOf(temperature));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = Integer.parseInt(dataInput.readUTF());
        temperature = Float.parseFloat(dataInput.readUTF());
    }
}
