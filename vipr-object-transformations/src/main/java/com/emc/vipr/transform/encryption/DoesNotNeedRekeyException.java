/**
 * 
 */
package com.emc.vipr.transform.encryption;

import com.emc.vipr.transform.TransformException;

/**
 * This exception is thrown from the rekey() method when the object is already using the
 * latest master key and does not need to be rekeyed.
 */
public class DoesNotNeedRekeyException extends TransformException {
    private static final long serialVersionUID = 8594710065359502592L;

    /**
     * @param message
     */
    public DoesNotNeedRekeyException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public DoesNotNeedRekeyException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public DoesNotNeedRekeyException(String message, Throwable cause) {
        super(message, cause);
    }

}
