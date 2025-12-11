package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class DoublePair implements WritableComparable<DoublePair> {
    private Text decade;
    private DoubleWritable score;

    public DoublePair(){
        decade = new Text();
        score = new DoubleWritable();
    }
    public DoublePair(Text decade, DoubleWritable score){
        this.decade = decade;
        this.score = score;
    }

    @Override
    public int compareTo(DoublePair other) {
        int cmp = this.decade.compareTo(other.decade);
        if (cmp != 0) {
            return cmp;
        }
        //multiply by -1 to flip the order
        return -1 * this.score.compareTo(other.score);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        decade.write(out);
        score.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        decade.readFields(in);
        score.readFields(in);
    }


    // --- GETTERS & SETTERS ---

    // Decade Getter
    public Text getDecade() {
        return decade;
    }

    // Decade Setter (Hadoop Text)
    public void setDecade(Text decade) {
        this.decade = decade;
    }
    
    // Decade Setter (Java String - Convenience)
    public void setDecade(String decadeStr) {
        this.decade.set(decadeStr);
    }

    // Score Getter
    public DoubleWritable getScore() {
        return score;
    }

    // Score Setter (Hadoop DoubleWritable)
    public void setScore(DoubleWritable score) {
        this.score = score;
    }

    // Score Setter (Java double - Convenience)
    public void setScore(double scoreVal) {
        this.score.set(scoreVal);
    }

    // Combined Setter (Optimization for re-using objects in Mapper)
    public void set(String decadeStr, double scoreVal) {
        this.decade.set(decadeStr);
        this.score.set(scoreVal);
    }
}