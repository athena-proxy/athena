package me.ele.jarch.athena.worker;

public enum TaskPriority {

    HIGH(0), ABOVE(1), NORMAL(2), LOW(3);

    private int value;

    private TaskPriority(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

}
