package me.ele.jarch.athena.scheduler;

/**
 * Created by jinghao.wang on 16/1/6.
 */
public class IdDecorator<T> {
    private final T instance;

    public IdDecorator(T instance) {
        this.instance = instance;
    }

    @SuppressWarnings("unchecked") @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;

        return this.instance == ((IdDecorator<T>) o).instance;
    }

    @Override public int hashCode() {
        return System.identityHashCode(instance);
    }

}
