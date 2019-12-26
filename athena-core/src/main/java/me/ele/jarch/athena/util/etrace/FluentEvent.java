package me.ele.jarch.athena.util.etrace;

import io.etrace.common.modal.Event;
import io.etrace.common.modal.impl.DummyEvent;

/**
 * 包装etrace的io.etrace.common.modal.Event对象，
 * 提供流式接口方便使用。
 */
public class FluentEvent {
    public static final FluentEvent NOOP = new FluentEvent(new DummyEvent());
    private final Event event;

    public FluentEvent(Event event) {
        this.event = event;
    }

    /**
     * add tag for event
     *
     * @param key
     * @param value
     * @return current FluentEvent
     */
    public FluentEvent tag(String key, String value) {
        event.addTag(key, value);
        return this;
    }

    /**
     * set data for event
     *
     * @param data
     * @return current FluentEvent
     */
    public FluentEvent data(String data) {
        event.setData(data);
        return this;
    }

    /**
     * set status for event
     * "0" is success, otherwise failure
     *
     * @param status
     * @return current FluentEvent
     */
    public FluentEvent status(String status) {
        event.setStatus(status);
        return this;
    }

    /**
     * set status for event
     * call this or call {@code status(String status)}
     *
     * @param t exception
     * @return current FluentEvent
     */
    public FluentEvent status(Throwable t) {
        event.setStatus(t);
        return this;
    }

    /**
     * submit current event
     */
    public void complete() {
        event.complete();
    }
}
