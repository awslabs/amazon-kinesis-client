package software.amazon.kinesis.leases.exceptions;

public class CustomerApplicationException extends Exception {

    public CustomerApplicationException(Throwable e) { super(e);}

    public CustomerApplicationException(String message, Throwable e) { super(message, e);}

    public CustomerApplicationException(String message) { super(message);}
}
