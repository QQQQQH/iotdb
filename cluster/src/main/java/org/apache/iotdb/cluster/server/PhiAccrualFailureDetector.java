package org.apache.iotdb.cluster.server;

import org.apache.iotdb.cluster.config.ClusterDescriptor;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PhiAccrualFailureDetector {

    private double threshold;
    private double minStdDeviation;
    private long acceptableHeartbeatPause; // ms

    private HeartbeatHistory heartbeatHistory;
    private AtomicReference<Long> lastTimestamp = new AtomicReference<Long>();

    private PhiAccrualFailureDetector(double _threshold,
                                      int _maxSampleSize,
                                      double _minStdDeviation,
                                      long _acceptableHeartbeatPause,
                                      long _firstHeartEstimate) {
        threshold = _threshold;
        minStdDeviation = _minStdDeviation;
        acceptableHeartbeatPause = _acceptableHeartbeatPause;
        long stdDeviation = _firstHeartEstimate/4;
        heartbeatHistory = new HeartbeatHistory(_maxSampleSize);
        heartbeatHistory.add(_firstHeartEstimate-stdDeviation)
                        .add(_firstHeartEstimate+stdDeviation);
    }
    private double ensureValidStdDeviation(double stdDeviation) {
        return Math.max(stdDeviation, minStdDeviation);
    }
    public synchronized double phi(long timestamp) {
        Long lastTimestamp = this.lastTimestamp.get();
        if(lastTimestamp == null) return 0.0;

        long timeDiff = timestamp-lastTimestamp;
        double mean = heartbeatHistory.mean()+acceptableHeartbeatPause;
        double stdDeviation = Math.max(heartbeatHistory.stdDeviation(), minStdDeviation);

        double y = (timeDiff-mean)/stdDeviation;
        double e = Math.exp(-y*(1.5976 + 0.070566*y*y));

        if(timeDiff > mean) return -Math.log10(e/(1.0 + e));
        else return -Math.log10(1.0-1.0/(1.0 + e));
    }
    public double phi() {
        return phi(System.currentTimeMillis());
    }
    public boolean isAvailable(long timestamp) {
        return phi(timestamp) < threshold;
    }
    public boolean isAvailable() {
        return phi(System.currentTimeMillis()) < threshold;
    }
    public synchronized void heartbeat(long timestamp) {
        Long lastTimestamp = this.lastTimestamp.getAndSet(timestamp);
        if(lastTimestamp != null) {
            long interval = timestamp-lastTimestamp;
            if(isAvailable(timestamp)) heartbeatHistory.add(interval);
        }
    }
    public void heartbeat() {
        heartbeat(System.currentTimeMillis());
    }

    public static class Builder {
        private double threshold = ClusterDescriptor.getInstance().getConfig().getThreshold();
        private int maxSampleSize = ClusterDescriptor.getInstance().getConfig().getMaxSampleSize();
        private double minStdDeviation = ClusterDescriptor.getInstance().getConfig().getMinStdDeviation();
        private long acceptableHeartbeatPause = ClusterDescriptor.getInstance().getConfig().getAcceptableHeartbeatPause();
        private long firstHeartbeatEstimate = ClusterDescriptor.getInstance().getConfig().getFirstHeartbeatEstimate();

        public Builder setThreshold(double _threshold) {
            threshold = _threshold;
            return this;
        }
        public Builder setMaxSampleSize(int _maxSampleSize) {
            maxSampleSize = _maxSampleSize;
            return this;
        }
        public Builder setMinStdDeviation(double _minStdDeviation) {
            minStdDeviation = _minStdDeviation;
            return this;
        }
        public Builder setAcceptableHeartbeatPauseMillis(long _acceptableHeartbeatPause) {
            acceptableHeartbeatPause = _acceptableHeartbeatPause;
            return this;
        }
        public Builder setFirstHeartbeatEstimateMillis(long _firstHeartbeatEstimate) {
            firstHeartbeatEstimate = _firstHeartbeatEstimate;
            return this;
        }
        public PhiAccrualFailureDetector build() {
            return new PhiAccrualFailureDetector(threshold,
                    maxSampleSize,
                    minStdDeviation,
                    acceptableHeartbeatPause,
                    firstHeartbeatEstimate);
        }
    }

    private static class HeartbeatHistory {
        private int maxSampleSize;
        private LinkedList<Long> intervals = new LinkedList<Long>();
        private AtomicLong intervalSum = new AtomicLong();
        private AtomicLong squaredIntervalSum = new AtomicLong();

        public HeartbeatHistory(int _maxSampleSize) { maxSampleSize = _maxSampleSize; }

        double mean() { return (double) intervalSum.get()/intervals.size(); }
        double variance() {
            double mean = mean();
            return ((double)squaredIntervalSum.get()/intervals.size())-mean*mean;
        }
        double stdDeviation() { return Math.sqrt(variance()); }
        public HeartbeatHistory add(long interval) {
            if(intervals.size() >= maxSampleSize) {
                Long intervalPolled = intervals.pollFirst();
                intervalSum.addAndGet(-intervalPolled);
                squaredIntervalSum.addAndGet(-intervalPolled*intervalPolled);
            }
            intervals.add(interval);
            intervalSum.addAndGet(interval);
            squaredIntervalSum.addAndGet(interval);
            return this;
        }
    }
}
