package com.neuron.core.http;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.ResourceLeakTracker;

public class HTTPClientNeuronResponse extends AbstractReferenceCounted {
	private static final ResourceLeakDetector<HTTPClientNeuronResponse> LEAK_DETECT = ResourceLeakDetectorFactory.instance().newResourceLeakDetector(HTTPClientNeuronResponse.class);
	private final ResourceLeakTracker<HTTPClientNeuronResponse> m_tracker;
	private final boolean m_protocolSuccess;
	private int m_httpStatusCode;
	private String m_httpStatusText;
	private ByteBuf m_responseData;
	
	public HTTPClientNeuronResponse(boolean protocolSuccess) {
		m_tracker = LEAK_DETECT.track(this);
		m_protocolSuccess = protocolSuccess;
	}
	
	@Override
	public ReferenceCounted touch(Object hint) {
		if (m_tracker != null) {
			m_tracker.record(hint);
		}
		return this;
	}
	@Override
	protected void deallocate() {
		if (m_tracker != null) {
			m_tracker.close(this);
		}
		if (m_responseData != null) {
			m_responseData.release();
		}
	}

	/**
	 * This flag indicates whether we successfully communicated with the server and exchanged valid data.
	 * This has no bearing on the content of the data, whether the HTTP status was 200 or 500... this just
	 * means we had successful I/O, SSL, compression, chunking, etc.
	 * 
	 * @return true if we had a valid HTTP session
	 */
	public boolean getProtocolSuccess() {
		return m_protocolSuccess;
	}
	
	void setStatusCode(int status) {
		m_httpStatusCode = status;
	}
	public int getHTTPStatusCode() {
		return m_httpStatusCode;
	}
	
	void setStatusText(String status) {
		m_httpStatusText = status;
	}
	public String getHTTPStatusText() {
		return m_httpStatusText;
	}
	

	void setResponseData(ByteBuf buf) {
		m_responseData = buf.retain();
	}

	/**
	 * The response object owns a reference count on the buffer.  If you want to keep the buffer after
	 * releasing the response, you need to call retain before releasing this object.
	 * 
	 * @return the response buffer instance or null if there is none
	 */
	public ByteBuf getResponseData() {
		return m_responseData;
	}
}
