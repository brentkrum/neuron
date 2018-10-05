package com.neuron.core;

import io.netty.buffer.ByteBuf;

public abstract class BytePipeBufWriter implements AutoCloseable {

	public BytePipeBufWriter() {
	}
	
	/**
	 * This is an accessor to the private ByteBuf for this writer instance.  You may
	 * read/writer this buffer in any way you like.  Calling close or tryClose will
	 * submit this buffer into the pipe.  You should not use the ByteBuf after close
	 * or tryClose is called.  Any reference to the private ByteBuf after
	 * close or tryClose is called will result in unpredictable failures.
	 * 
	 * @return
	 */
	public abstract ByteBuf buffer();
	
	public abstract boolean tryClose();
	
	public abstract void close() throws PipeFullException;
}
