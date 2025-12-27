package dsp2;

public class likelihoodRatio {

    // Stable log-likelihood for Binomial (without combinatorial term; it cancels in the ratio)
    private static double logL(double k, double n, double x) {
        // Guard against log(0)
        if (x <= 0.0) x = 1e-15;
        if (x >= 1.0) x = 1.0 - 1e-15;
        return k * Math.log(x) + (n - k) * Math.log(1.0 - x);
    }

    /**
     * Standard G^2 log-likelihood ratio (>= 0):
     * G^2 = 2 * ( log L(alt) - log L(null) )
     *
     * @param c1  occurrences of w1
     * @param c2  occurrences of w2
     * @param c12 occurrences of w1 with w2
     * @param N   total tokens in corpus (for the decade)
     */
    public static double get(long c1, long c2, long c12, long N) {
        double p  = (double) c2 / (double) N;
        double p1 = (double) c12 / (double) c1;
        double p2 = (double) (c2 - c12) / (double) (N - c1);

        double alt  = logL(c12, c1, p1) + logL(c2 - c12, N - c1, p2);
        double nul  = logL(c12, c1, p)  + logL(c2 - c12, N - c1, p);

        return 2.0 * (alt - nul);  // 
    }
}