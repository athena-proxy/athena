package me.ele.jarch.athena.detector;

import com.google.common.collect.EvictingQueue;

/**
 * Created by Dal-Dev-Team on 17/6/19.
 */
public class GroupAvg {
    // 过去30个时间窗口的平均响应时间
    public final EvictingQueue<Avg> queue = EvictingQueue.create(30);
    private double avgDur = 0.0;


    private static class Avg {
        private long sum = 0;
        private long count = 0;

        Avg(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        @Override public String toString() {
            StringBuilder sb = new StringBuilder("Avg{");
            sb.append("sum=").append(sum);
            sb.append(", count=").append(count);
            sb.append('}');
            return sb.toString();
        }
    }

    public void putData(long durSum, long count) {
        Avg avg = new Avg(durSum, count);
        queue.add(avg);
        avgDur = queue.stream().mapToLong(x -> x.sum).sum() / (double) queue.stream()
            .mapToLong(x -> x.count).sum();
    }

    public double getAvgDur() {
        return avgDur;
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder("GroupAvg{");
        sb.append("queue={");
        queue.forEach(x -> sb.append(x.toString()));
        sb.append("}");
        sb.append(", avgDur=").append(avgDur);
        sb.append('}');
        return sb.toString();
    }
}
