package TDE_2.question08;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CountryYearWritable implements WritableComparable<CountryYearWritable> {

    private String country;
    private String year;

    public CountryYearWritable() {
    }

    public CountryYearWritable(String country, String year) {
        this.country = country;
        this.year = year;
    }

    public String getCountry() {
        return country;
    }

    public String getYear() {
        return year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(country);
        out.writeUTF(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country = in.readUTF();
        year = in.readUTF();
    }

    @Override
    public int compareTo(CountryYearWritable other) {
        int cmp = this.country.compareTo(other.country);
        if (cmp != 0) return cmp;
        return this.year.compareTo(other.year);
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CountryYearWritable that = (CountryYearWritable) obj;
        return country.equals(that.country) && year.equals(that.year);
    }

    @Override
    public int hashCode() {
        return country.hashCode() * 163 + year.hashCode();
    }

    @Override
    public String toString() {
        return country + "\t" + year;
    }
}
