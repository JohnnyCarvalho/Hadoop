package advanced.entropy;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;

public class BaseQtdWritable implements Writable{

    private String base;
    private Long qtd;

    public BaseQtdWritable(String base, Long qtd) {
        this.base = base;
        this.qtd = qtd;
    }

    public String getBase() {
        return base;
    }

    public Long getQtd() {
        return qtd;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public void setQtd(Long qtd) {
        this.qtd = qtd;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(base);
        dataOutput.writeUTF(String.valueOf(qtd));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        base = dataInput.readUTF();
        qtd = Long.parseLong(dataInput.readUTF());
    }
}
