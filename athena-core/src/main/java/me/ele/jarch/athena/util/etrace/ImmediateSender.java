package me.ele.jarch.athena.util.etrace;

public class ImmediateSender extends ImpatientMetricsSender {

    private static final ImmediateSender SENDER = new ImmediateSender();

    public ImmediateSender() {
        super(null);
    }

    @Override protected boolean canSend() {
        return true;
    }

    public static ImmediateSender getImmediateSender() {
        return SENDER;
    }
}
