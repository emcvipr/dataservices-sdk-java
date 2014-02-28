/**
 * 
 */
package com.emc.vipr.transform;

/**
 * @author cwikj
 *
 */
public class TransformException extends Exception {
    private static final long serialVersionUID = -1990401831904476578L;

    public TransformException(String message) {
        super(message);
    }

    public TransformException(Throwable cause) {
        super(cause);
    }

    public TransformException(String message, Throwable cause) {
        super(message, cause);
    }

}
