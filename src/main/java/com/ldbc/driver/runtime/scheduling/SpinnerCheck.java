package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;

public interface SpinnerCheck {
    /**
     * Once a check has returned true it may never again return false
     *
     * @return
     */
    boolean doCheck();


    /**
     * Only called if check fails
     * Return value dictates if operation may still be executed, or if execution should be aborted.
     *
     * @param operation
     * @return operation may still be executed
     */
    boolean handleFailedCheck(Operation<?> operation);
}