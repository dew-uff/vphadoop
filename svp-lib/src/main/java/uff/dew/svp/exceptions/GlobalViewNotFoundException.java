package uff.dew.svp.exceptions;

public class GlobalViewNotFoundException extends Exception {

	public GlobalViewNotFoundException() {
		super();
	}

	public GlobalViewNotFoundException(String message) {
		super(message);
	}

	public GlobalViewNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	public GlobalViewNotFoundException(Throwable cause) {
		super(cause);
	}
}
