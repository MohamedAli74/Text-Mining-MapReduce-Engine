package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class collocation implements WritableComparable<collocation>{
    
    private Text decade;
    private Text word1;
    private Text word2;

    public collocation(){
        decade = new Text();
        word1 = new Text();
        word2 = new Text();
    }

    public collocation(Text decade, Text word1, Text word2){
        this.decade = decade;
        this.word1 = word1;
        this.word2 = word2;
    }
    public void set(Text decade, Text word1, Text word2){
        this.decade = decade;
        this.word1 = word1;
        this.word2 = word2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade.readFields(in);                
        word1.readFields(in);                
        word2.readFields(in);                
    }

    @Override
    public void write(DataOutput out) throws IOException {
        decade.write(out);
        word1.write(out);
        word2.write(out);     
    }

    @Override
    public int compareTo(collocation other) {
        int cmp = this.decade.compareTo(other.decade);
        if (cmp != 0) {
            return cmp;
        }
        
        cmp = this.word1.compareTo(other.word1);
        if (cmp != 0) {
            return cmp;
        }
        
        return this.word2.compareTo(other.word2);
    }
    
    // 4.for the Reducer to read the data
    public Text getDecade() { return decade; }
    public Text getWord1()  { return word1; }
    public Text getWord2()  { return word2; }

    public String toString(){
        return decade.toString() + " " + word1.toString() + " " + word2.toString();
    }

}
