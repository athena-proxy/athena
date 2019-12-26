package me.ele.jarch.athena.exception;

public class PacketTooLargeException extends QuitException {

    private static final long serialVersionUID = -162862259312150403L;

    public PacketTooLargeException(String message) {
        super(message);
    }

    public PacketTooLargeException(String message, Exception cause) {
        super(message, cause);
    }
}
