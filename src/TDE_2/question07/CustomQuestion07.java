package TDE_2.question07;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CustomQuestion07 implements Writable {
    private double soma;
    private int contagem;

    public CustomQuestion07() {}

    public CustomQuestion07(double soma, int contagem) {
        this.soma = soma;
        this.contagem = contagem;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeInt(contagem);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        contagem = in.readInt();
    }

    protected double getSoma() { return soma; }

    protected int getContagem() { return contagem; }

    protected void add(CustomQuestion07 other) {
        this.soma += other.soma;
        this.contagem += other.contagem;
    }

    @Override
    public String toString() {
        return soma + "\t" + contagem;
    }
}
