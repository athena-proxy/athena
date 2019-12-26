package me.ele.jarch.athena.util.common;

/**
 * 只读的Boolean类,用于某些做且只做一次的场景.
 * 思想来源与{@code AtomicBoolean}, 但与其不同的是更新操作不保证线程安全.
 * 相比AtomicBoolean,此类的优势在于更加轻量,
 * 类名借鉴只读存储器(Read-Only Memory，ROM).特性是一旦存储数据就无法再将之改变或删除
 *
 * @NonThreadSafe Created by jinghao.wang on 17/7/18.
 */
public class ReadOnlyBoolean {
    private boolean value;

    public ReadOnlyBoolean(boolean value) {
        this.value = value;
    }

    public ReadOnlyBoolean() {
    }

    public final boolean get() {
        return value;
    }

    public final void set(boolean newValue) {
        this.value = newValue;
    }

    /**
     * sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    public final boolean compareAndSet(boolean expect, boolean update) {
        if (value == expect) {
            value = update;
            return true;
        } else {
            return false;
        }
    }

    @Override public String toString() {
        return Boolean.toString(value);
    }
}
