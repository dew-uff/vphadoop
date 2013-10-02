package uff.dew.vphadoop.client;

public class JobException extends Exception {

    private static final long serialVersionUID = -8814326689355005839L;

    @Override
    public String getMessage() {
        return "Job failed! Cause: " + super.getMessage();
    }
}
