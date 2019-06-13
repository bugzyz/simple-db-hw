package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int ntups;
    private int min;
    private int max;
    private double b_width;
    private int buckets;
    private int values[];

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // Done
        this.min = min;
        this.max = max;
        this.buckets = buckets;

        if (buckets > max - min + 1) {
            buckets = max - min + 1;
        }

        values = new int[buckets];
        this.b_width = (double) (max - min + 1) / buckets;
    }

    private int getIndex(int v) {
        return (int) ((v - min) / b_width);
    }

    private boolean inRange(int v) {
        if (v >= min && v <= max) {
            return true;
        }
        return false;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // Done
        ntups++;
        values[getIndex(v)] += 1;
    }
    
    private double getGreaterThanPointSelectivity(int v) {
        int b = getIndex(v);
        double b_right = (b + 1) * b_width;
        double b_f = ((double) values[b]) / ntups;
        double b_part = ((double) (b_right - v)) / b_width;
        double selectivity = b_f * b_part;
        for (int i = b + 1; i < buckets; i++) {
            selectivity += ((double) values[i]) / ntups;
        }
        return selectivity;
    }

    private double getLessThanPointSelectivity(int v) {
        int b = getIndex(v);
        double b_left = (b) * b_width;
        double b_f = ((double) values[b]) / ntups;
        double b_part = ((double) (v - b_left)) / b_width;
        double selectivity = b_f * b_part;
        for (int i = 0; i < b; i++) {
            selectivity += ((double) values[i]) / ntups;
        }
        return selectivity;
    }

    private double getOnePointSelectivity(int v) {
        int b = getIndex(v);

        return ((double) values[b]) / b_width / ntups;
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // Done
        switch (op) {
        case EQUALS:
            if (!inRange(v)) {
                return 0;
            }
            return getOnePointSelectivity(v);
        case GREATER_THAN:
            if (v >= max) {
                return 0;
            }
            if (v < min) {
                return 1;
            }
            return getGreaterThanPointSelectivity(v);
        case LESS_THAN:
            if (v > max) {
                return 1;
            }
            if (v <= min) {
                return 0;
            }
            return getLessThanPointSelectivity(v) - getOnePointSelectivity(v);
        case LESS_THAN_OR_EQ:
            if (v >= max) {
                return 1;
            }
            if (v < min) {
                return 0;
            }
            return getLessThanPointSelectivity(v);
        case GREATER_THAN_OR_EQ:
            if (v > max) {
                return 0;
            }
            if (v <= min) {
                return 1;
            }
            return getGreaterThanPointSelectivity(v) + getOnePointSelectivity(v);
        case NOT_EQUALS:
            if (!inRange(v)) {
                return 1;
            }
            return 1 - getOnePointSelectivity(v);
        default:
            return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // Done
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            sb.append("values[" + i + "]=" + values[i]);
        }
        return sb.toString();
    }
}
