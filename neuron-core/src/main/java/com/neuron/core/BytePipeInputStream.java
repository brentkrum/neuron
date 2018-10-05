package com.neuron.core;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;

public final class BytePipeInputStream extends InputStream implements AutoCloseable, DataInput {
	public enum LineEnding { CR, CRLF };

	private static final Charset CHARSET_ASCII = Charset.forName("US-ASCII");
	private static final int DEFAULT_MAX_STRING_SIZE;
	
	private final ByteBuf m_readBuffer;
	private int MAX_STRING_SIZE = DEFAULT_MAX_STRING_SIZE;
	private LineEnding LINE_ENDING = LineEnding.CRLF;
	
	static {
		DEFAULT_MAX_STRING_SIZE = Config.getFWInt("core.BytePipeInputStream.defaultMaxStringSize", 1024);
	}
	
	BytePipeInputStream(ByteBuf buf) {
		m_readBuffer = buf;
	}
	
	public void setMaxStringSize(int size) {
		MAX_STRING_SIZE = size;
	}
	public void setLineEnding(LineEnding lineEnding) {
		LINE_ENDING = lineEnding;
	}
	
	@Override
	public void readFully(byte[] b) throws IOException
	{
		readFully(b, 0, b.length);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException
	{
		int numRead = read(b, off, len);
		if (numRead != len) {
			throw new EOFException();
		}
	}

	@Override
	public int skipBytes(int numTryToSkip) throws IOException
	{
		if (numTryToSkip <= 0) {
			return 0;
		}
		final int readable = m_readBuffer.readableBytes();
		final int toSkip = (readable > numTryToSkip) ? numTryToSkip : readable;
		m_readBuffer.skipBytes(toSkip);
		return toSkip;
	}

	@Override
	public boolean readBoolean() throws IOException
	{
		return (readUnsignedByte() != 0);
	}

	@Override
	public byte readByte() throws IOException
	{
		if (m_readBuffer.readableBytes() < 1) {
			throw new EOFException();
		}
		return m_readBuffer.readByte();
	}

	@Override
	public int readUnsignedByte() throws IOException
	{
		if (m_readBuffer.readableBytes() < 1) {
			throw new EOFException();
		}
		return m_readBuffer.readUnsignedByte();
	}

	@Override
	public short readShort() throws IOException
	{
		if (m_readBuffer.readableBytes() < Short.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readShort();
	}
	public short readShortLE() throws IOException
	{
		if (m_readBuffer.readableBytes() < Short.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readShortLE();
	}

	@Override
	public int readUnsignedShort() throws IOException
	{
		if (m_readBuffer.readableBytes() < Short.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readUnsignedShort();
	}
	public int readUnsignedShortLE() throws IOException
	{
		if (m_readBuffer.readableBytes() < Short.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readUnsignedShortLE();
	}

	@Override
	public char readChar() throws IOException
	{
		if (m_readBuffer.readableBytes() < Character.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readChar();
	}

	@Override
	public int readInt() throws IOException
	{
		if (m_readBuffer.readableBytes() < Integer.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readInt();
	}
	public int readIntLE() throws IOException
	{
		if (m_readBuffer.readableBytes() < Integer.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readIntLE();
	}
	public long readUnsignedInt() throws IOException
	{
		if (m_readBuffer.readableBytes() < Integer.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readUnsignedInt();
	}
	public long readUnsignedIntLE() throws IOException
	{
		if (m_readBuffer.readableBytes() < Integer.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readUnsignedIntLE();
	}

	@Override
	public long readLong() throws IOException
	{
		if (m_readBuffer.readableBytes() < Long.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readLong();
	}
	public long readLongLE() throws IOException
	{
		if (m_readBuffer.readableBytes() < Long.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readLongLE();
	}

	@Override
	public float readFloat() throws IOException
	{
		if (m_readBuffer.readableBytes() < Float.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readFloat();
	}
	public float readFloatLE() throws IOException
	{
		if (m_readBuffer.readableBytes() < Float.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readFloatLE();
	}

	@Override
	public double readDouble() throws IOException
	{
		if (m_readBuffer.readableBytes() < Double.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readDouble();
	}
	public double readDoubleLE() throws IOException
	{
		if (m_readBuffer.readableBytes() < Double.BYTES) {
			throw new EOFException();
		}
		return m_readBuffer.readDoubleLE();
	}

	public String readLineCR() throws IOException
	{
		final int startIndex = m_readBuffer.readerIndex();
		final int endIndex = m_readBuffer.forEachByte(ByteProcessor.FIND_CR);
		final String s;
		if (endIndex == -1) {
			if (m_readBuffer.readableBytes() < MAX_STRING_SIZE) {
				return null;
			}		
			s = m_readBuffer.toString(startIndex, MAX_STRING_SIZE, CHARSET_ASCII);
		} else {
			final int len = endIndex - startIndex;
			s = m_readBuffer.toString(startIndex, len, CHARSET_ASCII);
			// Skip the CR
			m_readBuffer.skipBytes(1);
		}
		return s;
	}

	public String readLineCRLF() throws IOException
	{
		final int startIndex = m_readBuffer.readerIndex();
		final int readableBytes = m_readBuffer.readableBytes();
		int searchIndex = startIndex;
		while(true) {
			final int endIndex = m_readBuffer.forEachByte(searchIndex, readableBytes, ByteProcessor.FIND_CR);
			final String s;
			if (endIndex == -1) {
				if (readableBytes < MAX_STRING_SIZE) {
					return null;
				}		
				s = m_readBuffer.toString(startIndex, MAX_STRING_SIZE, CHARSET_ASCII);
			} else {
				final int len = endIndex - startIndex;
				if (len+1 > readableBytes) {
					return null;
				}
				if (m_readBuffer.getUnsignedByte(endIndex+1) == '\n') {
					s = m_readBuffer.toString(startIndex, len, CHARSET_ASCII);
					// Skip the CRLF
					m_readBuffer.skipBytes(2);
					return s;
				}
				// We have a CR without a LF... skip it
				searchIndex = endIndex+1;
			}
		}
	}
	
	@Override
	public String readLine() throws IOException
	{
		if (LINE_ENDING == LineEnding.CRLF) {
			return readLineCRLF();
		} else {
			return readLineCR();
		}
	}

	@Override
	public String readUTF() throws IOException
	{
		throw new IOException("readUTF() not supported, use a ByteBufPipe instead of a ByteStreamPipe and parse it appropriately.");
	}

	@Override
	public int read() throws IOException
	{
		if (m_readBuffer.readableBytes() == 0) {
			return -1;
		}
		return m_readBuffer.readUnsignedByte();
	}

	@Override
	public int read(byte[] b) throws IOException
	{
		return read(b, 0, b.length);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException
	{
		final int readable = m_readBuffer.readableBytes();
		final int length = (readable > len) ? len : readable;
		m_readBuffer.readBytes(b, off, length);
		return length;
	}

	@Override
	public long skip(long numTryToSkip) throws IOException
	{
		if (numTryToSkip > Integer.MAX_VALUE) {
			return super.skip(numTryToSkip);
		}
		return skipBytes((int)numTryToSkip);
	}

	@Override
	public int available() throws IOException
	{
		return m_readBuffer.readableBytes();
	}

	@Override
	public boolean markSupported()
	{
		return false;
	}

	@Override
	public void close()
	{
		// This method does nothing
	}		
}
