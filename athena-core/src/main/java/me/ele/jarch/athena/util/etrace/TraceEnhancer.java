package me.ele.jarch.athena.util.etrace;

import io.etrace.agent.Trace;

/**
 * enhance io.etrace.agent.Trace for easy use
 */
public class TraceEnhancer {
    /**
     * fluent trace event wrapper
     * <p>
     * TraceEnhancer.newFluentEvent("type", name).tag("key", "val1").data("data").status("warning").complete();
     *
     * @param type
     * @param name
     * @return FluentEvent
     */
    public static FluentEvent newFluentEvent(String type, String name) {
        return new FluentEvent(Trace.newEvent(type, name));
    }
}
