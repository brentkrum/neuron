package com.neuron.core.http.netty;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;

/**
 * 
 * 
 * There is no need to synchronization within the class, since there is only a single thread calling any
 * of the methods, including all the async callbacks.
 * 
 * @author brentk
 *
 */
public class ChunkedAsyncNioFile implements ChunkedInput<ByteBuf> {
	private static final Logger LOG = LogManager.getLogger(ChunkedAsyncNioFile.class);
	private static final ReadCompletionHandler m_readCompletion = new ReadCompletionHandler();
	private static final Set<OpenOption> FILE_READ_OPTIONS = new HashSet<>(Arrays.asList(StandardOpenOption.READ));
	private final File m_file;
	private final AsynchronousFileChannel m_in;
	private final ChunkedWriteHandler m_writeHandler;
	private final long m_chunkSize;
	private final long m_fileLength;
	
	private boolean m_kickedOffAsyncReading;
	private ByteBufAllocator m_bbAllocator;
	
	private boolean m_closed;
	private boolean m_eof;
	private boolean m_endOfInput;
	private ByteBuf m_currentChunk;
	private ByteBuf m_nextChunk;
	private Exception m_caughtException;
	
	private long m_position;
	private ByteBuf m_ioChunk;
	private ByteBuffer m_ioChunkBB;
	
	public ChunkedAsyncNioFile(File f, ExecutorService executor, ChunkedWriteHandler writeHandler, int chunkSize) throws IOException {
		m_file = f;
		m_in = AsynchronousFileChannel.open(f.toPath(), FILE_READ_OPTIONS, executor);
		m_fileLength = m_in.size();
		m_writeHandler = writeHandler;
		m_chunkSize = chunkSize;
	}

	@Override
	public long length() {
		return m_fileLength;
	}

	@Override
	public /*synchronized*/ long progress() {
		return m_position;
	}

	@Override
	public boolean isEndOfInput() throws Exception {
		return m_endOfInput;
	}

	@Override
	public void close() throws Exception {
		/* synchronized(this) */ {
			m_closed = true;
			if (m_currentChunk != null) {
				m_currentChunk.release();
				m_currentChunk = null;
			}
			if (m_nextChunk != null) {
				m_nextChunk.release();
				m_nextChunk = null;
			}
			// The m_ioChunk should be handled when the callback is called
		}
		m_in.close();
	}

	@Override
	public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
		return readChunk(ctx.alloc());
	}

	@Override
	public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
		if (!m_kickedOffAsyncReading) {
			m_kickedOffAsyncReading = true;
			m_bbAllocator = allocator;
			startRead();
			return null;
		}
		if (m_endOfInput) {
			return null;
		}
		boolean doStartRead = false;
		try {
			/* synchronized(this) */ {
				if (m_caughtException != null) {
					final Exception ex = m_caughtException;
					m_caughtException = null;
					throw ex;
				}
				if (m_currentChunk != null) {
					final ByteBuf buf = m_currentChunk;
					if (m_nextChunk != null) {
						m_currentChunk = m_nextChunk;
						m_nextChunk = null;
						// If m_nextChunk is set, then we do not have a pending read operation
						// So, start one if we are not already at EOF 
						doStartRead = !m_eof;
					} else {
						m_currentChunk = null;
						if (m_eof) {
							m_endOfInput = true;
						}
					}
					return buf;
				}
			}
		} finally {
			if (doStartRead) {
				startRead();
			}
		}
		// Async I/O is still outstanding
		return null;
	}
	
	private void startRead() {
		final int chunkSize = (int) Math.min(m_chunkSize, m_fileLength - m_position);
		m_ioChunk = m_bbAllocator.directBuffer(chunkSize);
		m_ioChunkBB = m_ioChunk.nioBuffer();
		if (m_ioChunkBB.position() != 0) {
			LOG.fatal("ByteBuf.nioBuffer() returned a buffer who's position() was not 0", new RuntimeException("This was unexpected and needs to be fixed"));
		}
		boolean releaseBuffer = true;
		try {
			m_in.read(m_ioChunkBB, m_position, this, m_readCompletion);
			releaseBuffer = false;
		} finally {
			if (releaseBuffer) {
				// Do this if read throws an exception
				m_ioChunk.release();
				m_ioChunk = null;
				m_ioChunkBB = null;
			}
		}
	}
	
	private void onEOF() {
		/* synchronized(this) */  {
			m_eof = true;
			m_ioChunk.writerIndex(m_ioChunkBB.position());
			if (m_currentChunk != null) {
				m_nextChunk = m_ioChunk;
			} else {
				m_currentChunk = m_ioChunk;
			}
			m_ioChunk = null;
			m_ioChunkBB = null;
		}
		// There is a m_currentChunk, tell the ChunkWriteHandler to process it
		m_writeHandler.resumeTransfer();
	}
	
	private void readCompleted(int bytesRead) {
		/* synchronized(this) */ {
			if (m_closed) {
				m_ioChunk.release();
				m_ioChunk = null;
				m_ioChunkBB = null;
				return;
			}
		}
		// EOF
		if (bytesRead == -1) {
			// This should not normally happen.  This will happen if for some reason the file we
			// were reading hit EOF before we read to the end of the file length.
			onEOF();
			LOG.error("Premature end of file.  When reading '{}' we reached EOF reading at position {} but the file length is {}", m_file, m_position, m_fileLength);
			return;
		}
		m_position += bytesRead;
		
		// If we have not read a full chunk, continue reading
		if (m_ioChunkBB.hasRemaining()) {
			try {
				m_in.read(m_ioChunkBB, m_position, this, m_readCompletion);
			} catch(Exception ex) {
				m_caughtException = ex;
				m_ioChunk.release();
				m_ioChunk = null;
				m_ioChunkBB = null;
				// There is a m_caughtException, tell the ChunkWriteHandler to process it
				m_writeHandler.resumeTransfer();
				return;
			}

		// We read a full chunk
		} else {
			if (m_position < m_fileLength) {
				final boolean doStartRead;
				/* synchronized(this) */ {
					m_ioChunk.writerIndex(m_ioChunkBB.position());
					if (m_currentChunk != null) {
						m_nextChunk = m_ioChunk;
						doStartRead = false;
					} else {
						m_currentChunk = m_ioChunk;
						doStartRead = !m_closed;
					}
					m_ioChunk = null;
					m_ioChunkBB = null;
				}
				if (doStartRead) {
					try {
						startRead();
					} catch(Exception ex) {
						/* synchronized(this) */ {
							m_caughtException = ex;
							m_eof = true;
							m_endOfInput = true;
						}
					}
				}
			} else {
				onEOF();
			}
			// There is a m_currentChunk, tell the ChunkWriteHandler to process it
			m_writeHandler.resumeTransfer();
		}
	}
	
	private void readFailed(Throwable t) {
		/* synchronized(this) */ {
			if (t instanceof Exception) {
				m_caughtException = (Exception)t;
			} else {
				m_caughtException = new RuntimeException(t);
			}
			if (m_ioChunk != null) {
				m_ioChunk.release();
				m_ioChunk = null;
				m_ioChunkBB = null;
			}
		}
		// There is a m_caughtException, tell the ChunkWriteHandler to process it
		m_writeHandler.resumeTransfer();
	}

	private static final class ReadCompletionHandler implements CompletionHandler<Integer,ChunkedAsyncNioFile> {

		@Override
		public void completed(Integer result, ChunkedAsyncNioFile attachment) {
			attachment.readCompleted(result);
		}

		@Override
		public void failed(Throwable exc, ChunkedAsyncNioFile attachment) {
			attachment.readFailed(exc);
		}
		
	}
}
