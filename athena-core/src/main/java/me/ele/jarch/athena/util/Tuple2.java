package me.ele.jarch.athena.util;

/**
 * Created by jinghao.wang on 16/8/8.
 */
public class Tuple2<A, B> {
    public final A a;
    public final B b;

    public Tuple2(A a, B b) {
        this.a = a;
        this.b = b;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;

        if (a != null ? !a.equals(tuple2.a) : tuple2.a != null)
            return false;
        return b != null ? b.equals(tuple2.b) : tuple2.b == null;

    }

    @Override public int hashCode() {
        int result = a != null ? a.hashCode() : 0;
        result = 31 * result + (b != null ? b.hashCode() : 0);
        return result;
    }

    @Override public String toString() {
        return "Tuple2{" + "a=" + a + ", b=" + b + '}';
    }
}
