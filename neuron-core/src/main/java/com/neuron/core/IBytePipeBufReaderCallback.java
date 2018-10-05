package com.neuron.core;

import io.netty.buffer.ByteBuf;

public interface IBytePipeBufReaderCallback {
	/**
	 * If this is a ReadPipeType of Chunk, this is your one chance of seeing
	 * this buffer.  If you want to keep the buffer, call the buf.retain()
	 * method.
	 * 
	 * If this is a ReadPipeType of AppendBuf, this same ByteBuf will be provided
	 * to this callback every time it is called.  If what is in the buffer is only
	 * a partial message or partial data, you may leave it there until the full
	 * message arrives.  Do not call the buf.retain() or buf.release() methods,
	 * since you do not own the buffer. Keeping references to this buffer or using
	 * this buffer from anywhere outside this call can result in system instability.
	 * 
	 */
	void onData(ByteBuf buf);
}
