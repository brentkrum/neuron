package com.neuron.utility;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
	private final ByteBuffer m_bb;

	public ByteBufferInputStream(ByteBuffer bb) {
		m_bb = bb;
	}

	@Override
	public int available() throws IOException {
		return m_bb.remaining();
	}

	@Override
	public int read(final byte[] b, final int off, int len) throws IOException {
		final int rem = m_bb.remaining();
		if (rem == 0) {
			return -1;
		}
		if (len > rem) {
			len = rem;
		}
		m_bb.get(b, off, len);
		return len;
	}

	@Override
	public int read(final byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public int read() throws IOException {
		if (m_bb.remaining() == 0) {
			return -1;
		}
		return (m_bb.get() & 0xFF);
	}
}
