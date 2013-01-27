package org.HdrHistogram;

public class HistogramPlaytime {

    /**
     * @param args
     */
    public static void main(String[] args) {
        //long a = 1000000000L;
        long a = (60 * 60 * 1000);
        int b = 1;

        Histogram hist2 = new Histogram(a, b);
        System.out.println(hist2.getEstimatedFootprintInBytes());

        IntHistogram hist = new IntHistogram( a, b);
        System.out.println(hist.getEstimatedFootprintInBytes());

        ShortHistogram hist3 = new ShortHistogram(a, b);
        System.out.println(hist3.getEstimatedFootprintInBytes());

        hist2.add(hist);

        hist2.recordValue(33);
        hist2.getHistogramData().getValueAtPercentile(.99);
    }

}
