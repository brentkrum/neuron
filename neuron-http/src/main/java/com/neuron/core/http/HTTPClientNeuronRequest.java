package com.neuron.core.http;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.ResourceLeakTracker;

public final class HTTPClientNeuronRequest extends AbstractReferenceCounted {
	private static final ResourceLeakDetector<HTTPClientNeuronRequest> LEAK_DETECT = ResourceLeakDetectorFactory.instance().newResourceLeakDetector(HTTPClientNeuronRequest.class);
	private final ResourceLeakTracker<HTTPClientNeuronRequest> m_tracker;
	private final String m_method;
	private final String m_url;
	  
	private List<NameValue> m_queryParams;
	
	private List<NameValue> m_headers;
	  
	private String m_stringData;
	private List<NameValue> m_formParams;

	private File m_responseBodyOutputFile;
	private Boolean m_followRedirect;
	private Integer m_requestTimeout;
	private Integer m_readTimeout;

	public HTTPClientNeuronRequest(String url) {
		m_method = "GET";
		m_url = url;
		m_tracker = LEAK_DETECT.track(this);
	}
	
	public HTTPClientNeuronRequest(String method, String url) {
		m_method = method;
		m_url = url;
		m_tracker = LEAK_DETECT.track(this);
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
	}

	public String getMethod() {
		return m_method;
	}
	public String getURL() {
		return m_url;
	}
	public String getBodyData() {
		return m_stringData;
	}
	public List<NameValue> getQueryParams() {
		return m_queryParams;
	}
	public List<NameValue> getHeaders() {
		return m_headers;
	}
	public List<NameValue> getFormParams() {
		return m_formParams;
	}
	public Boolean followRedirect() {
		return m_followRedirect;
	}
	public Integer requestTimeout() {
		return m_requestTimeout;
	}
	public Integer readTimeout() {
		return m_readTimeout;
	}
	public File getResponseBodyOutputFile() {
		return m_responseBodyOutputFile;
	}

	public void setResponseBodyOutputFile(File file) {
		m_responseBodyOutputFile = file;
	}
	
	public void addQueryParam(String name, String value) {
		if (m_queryParams == null) {
			m_queryParams = new ArrayList<>();
		}
		m_queryParams.add(new NameValue(name, value));
	}

	public void addHeader(String name, String value) {
		if (m_headers == null) {
			m_headers = new ArrayList<>();
		}
		m_headers.add(new NameValue(name, value));
	}
	
	public void addOrReplaceHeader(String name, String value) {
		if (m_headers == null) {
			m_headers = new ArrayList<>();
			m_headers.add(new NameValue(name, value));
		}
		for(int i=0; i<m_headers.size(); i++) {
			NameValue nv = m_headers.get(i);
			if (nv.name.equals(name)) {
				m_headers.set(i, new NameValue(name, value));
				return;
			}
		}
		m_headers.add(new NameValue(name, value));
	}

	public void addFormParam(String name, String value) {
		if (m_formParams == null) {
			m_formParams = new ArrayList<>();
			addOrReplaceHeader(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED.toString());
		}
		m_formParams.add(new NameValue(name, value));
	}
	
	public void addBodyData(String contentType, String data) {
		m_formParams = null;
		m_stringData = data;
		addOrReplaceHeader(HttpHeaderNames.CONTENT_TYPE.toString(), contentType);
	}
	
	public void addBodyData(String data) {
		m_formParams = null;
		m_stringData = data;
	}
	
	static class NameValue {
		final String name;
		final String value;
		
		NameValue(String name, String value) {
			this.name = name;
			this.value = value;
		}
	}
}
