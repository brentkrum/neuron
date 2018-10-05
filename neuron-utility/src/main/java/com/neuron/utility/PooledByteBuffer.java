package com.neuron.utility;

import java.nio.ByteBuffer;

public interface PooledByteBuffer extends AutoCloseable {

    /**
     * Free this resource for immediate re-use.  The resource must not be accessed again after
     * calling this method.
     */
    void free();

    /**
     * Get the buffer.
     *
     * @return the buffer
     */
    ByteBuffer get();

    /**
     * Delegates to {@link #free()}.
     */
    void close();
}
