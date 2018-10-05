package com.neuron.utility;

import java.io.OutputStream;
import java.nio.ByteBuffer;

public final class ByteBufferOutputStream extends OutputStream {

	private ByteBuffer m_buffer;

	public ByteBufferOutputStream(final ByteBuffer buffer) {

		m_buffer = buffer;
	}

	@Override
	public void write(int b) {
		m_buffer.put((byte)b);
	}

	@Override
	public void write(byte[]bytes, final int offset, final int len) {
		m_buffer.put(bytes, offset, len);
	}
}
