package dsp2;
public class likelihoodRatio {
    private double L(double k, double n, double x){
        return Math.pow(x, k) * Math.pow(1-x, n-k);
    }
    private double logL(double k, double n, double x){
        return Math.log(L(k, n, x));
    }
    /**
     * returns the log likelihood ratio of a given collocation's data
     * @param c1  the number of occurrences of w1
     * @param c2  the number of occurrences of w2
     * @param c12  the number of occurrences of w1@w2
     * @param N the total number of words in the corpus
     * @return log likelihood ratio of the collocation 
     */
    public double get(long c1, long c2, long c12, long N){
        double p = (double) c2 / N;
        double p1 = (double) c12/c1;
        double p2 = (double) (c2-c12)/(N-c1);
        return logL(c12,c1,p) + logL(c2-c12,N-c1,p) - logL(c12,c1,p1) - logL(c2-c12, N - c1, p2);
    }
}
