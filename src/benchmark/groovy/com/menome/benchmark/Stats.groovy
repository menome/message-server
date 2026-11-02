package com.menome.benchmark

class Stats {

    /** Nearest-rank percentile (p in 1..100); the value at that rank is always one that was observed. */
    static double percentile(List<Double> values, double p) {
        if (values.isEmpty()) return 0d
        List<Double> sorted = values.sort(false)
        int rank = (int) Math.ceil(p / 100d * sorted.size())
        sorted[Math.min(sorted.size(), Math.max(1, rank)) - 1]
    }
}
