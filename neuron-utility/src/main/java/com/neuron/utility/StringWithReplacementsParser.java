package com.neuron.utility;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class StringWithReplacementsParser {
	private final String m_openString;
	private final String m_closeString;
	private final String m_escapedOpenString;
	private final String m_escapedCloseString;
	
	public StringWithReplacementsParser(String openString, String closeString, String escapedOpenString, String escapedCloseString) {
		m_openString = openString;
		m_closeString = closeString;
		m_escapedOpenString = escapedOpenString;
		m_escapedCloseString = escapedCloseString;
	}
	
	public boolean hasReplacements(String s) {
		if (m_escapedOpenString == null) {
			return s.indexOf(m_openString) != -1;
		}
		int currentIndex = 0;
		while(true) {
			int openIndex = s.indexOf(m_openString, currentIndex);
			if (openIndex == -1) {
				return false;
			}
			int escapedOpenIndex = s.indexOf(m_escapedOpenString, currentIndex);
			if (escapedOpenIndex == -1) {
				return true;
			}
			int endEscaped = escapedOpenIndex + m_escapedOpenString.length();
			if (openIndex >= escapedOpenIndex && openIndex < endEscaped) {
				currentIndex = endEscaped;
				continue;
			}
			return true;
		}
	}
	
	private ParserBase parse(String s) {
		if (m_escapedOpenString != null) {
			return new EscapingParser(s);
		} else {
			return new NonEscapingParser(s);
		}
		
	}
	
	public String replace(String s, Map<String, String> replacements) {
		final ParserBase parser = parse(s);
		final StringBuilder sb = new StringBuilder();
		for(Object o : parser.m_stringParts) {
			if (o instanceof String) {
				sb.append((String)o);
			} else {
				final String name = ((ValueNeedingReplacement)o).name;
				final String value = replacements.get(name);
				if (value != null) {
					sb.append(value);
				} else {
					sb.append("{{{missing:").append(name).append("}}}");
				}
			}
		}
		return sb.toString();
	}
	
	public String replace(String s, ISupplyReplacement replacementSupplier) {
		final ParserBase parser = parse(s);
		final StringBuilder sb = new StringBuilder();
		for(Object o : parser.m_stringParts) {
			if (o instanceof String) {
				sb.append((String)o);
			} else {
				final String name = ((ValueNeedingReplacement)o).name;
				final String value = replacementSupplier.get(name);
				if (value != null) {
					sb.append(value);
				} else {
					sb.append("{{{missing:").append(name).append("}}}");
				}
			}
		}
		return sb.toString();
	}
	
	String dump_parse(String s) {
		final ParserBase parser = parse(s);
		final StringBuilder sb = new StringBuilder();
		boolean isFirst = true;
		for(Object o : parser.m_stringParts) {
			if (!isFirst) {
				sb.append(", ");
			}
			if (o instanceof String) {
				sb.append('"').append((String)o).append('"');
			} else {
				final String name = ((ValueNeedingReplacement)o).name;
				sb.append(m_openString).append(name).append(m_closeString);
			}
			isFirst = false;
		}
		return sb.toString();
	}
	
	private int isString(String matchString, String inputString, int index, int len) {
		final int matchStringLen = matchString.length();
		final int inputStringLen = inputString.length();
		for(int i=0; i<matchStringLen; i++) {
			if (index >= inputStringLen) {
				return -1;
			}
			if (matchString.charAt(i) != inputString.charAt(index++)) {
				return -1;
			}
		}
		return index;
	}
	
	private class ParserBase {
		protected final List<Object> m_stringParts = new LinkedList<>();
		protected final StringBuilder m_currentPart;
		
		public ParserBase(int len) {
			m_currentPart = new StringBuilder(len);
		}
		
		protected void addCurrentPart() {
			if (m_currentPart.length() != 0) {
				m_stringParts.add(m_currentPart.toString());
				m_currentPart.setLength(0);
			}
		}
	}
	
	private final class NonEscapingParser extends ParserBase {
		
		public NonEscapingParser(String s) {
			super(s.length());
			final int len = s.length();
			int index = 0;
			int nextIndex;
			while(index < len) {
				nextIndex = isString(m_openString, s, index, len);
				if (nextIndex != -1) {
					addCurrentPart();
					index = parseOpenedToken(s, nextIndex, len);
				} else {
					m_currentPart.append(s.charAt(index++));
				}
			}
			addCurrentPart();
		}
		
		private int parseOpenedToken(String s, int index, int len) {
			while(index < len) {
				int nextIndex = isString(m_closeString, s, index, len);
				if (nextIndex != -1) {
					index = nextIndex;
					break;
				}
				m_currentPart.append(s.charAt(index++));
			}
			if (m_currentPart.length() == 0) {
				throw new RuntimeException("String has an empty open/close pair at index " + index );
			}
			m_stringParts.add( new ValueNeedingReplacement(m_currentPart.toString()) );
			m_currentPart.setLength(0);
			return index;
		}
	}
	
	
	private final class EscapingParser extends ParserBase  {
		
		public EscapingParser(String s) {
			super(s.length());
			final int len = s.length();
			int index = 0;
			int nextIndex;
			while(index < len) {
				if (m_escapedOpenString != null) {
					nextIndex = isString(m_escapedOpenString, s, index, len);
					if (nextIndex != -1) {
						m_currentPart.append(m_openString);
						index = nextIndex;
						continue;
					}
				}
				nextIndex = isString(m_openString, s, index, len);
				if (nextIndex != -1) {
					addCurrentPart();
					index = parseOpenedToken(s, nextIndex, len);
				} else {
					m_currentPart.append(s.charAt(index++));
				}
			}
			addCurrentPart();
		}
		
		private int parseOpenedToken(String s, int index, int len) {
			while(index < len) {
				int nextIndex;
				if (m_escapedCloseString != null) {
					nextIndex = isString(m_escapedCloseString, s, index, len);
					if (nextIndex != -1) {
						m_currentPart.append(m_closeString);
						index = nextIndex;
						continue;
					}
				}
				nextIndex = isString(m_closeString, s, index, len);
				if (nextIndex != -1) {
					index = nextIndex;
					break;
				}
				m_currentPart.append(s.charAt(index++));
			}
			if (m_currentPart.length() == 0) {
				throw new RuntimeException("String has an empty open/close pair at index " + index );
			}
			m_stringParts.add( new ValueNeedingReplacement(m_currentPart.toString()) );
			m_currentPart.setLength(0);
			return index;
		}
	}
	
	public interface ISupplyReplacement {
		String get(String name);
	}
	private static class ValueNeedingReplacement {
		final String name;
		
		ValueNeedingReplacement(String name) {
			this.name = name;
		}
	}
}
