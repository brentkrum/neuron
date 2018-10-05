package com.neuron.utility;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StringWithReplacements {
	private static final Pattern REPLACEMENT_VALUE = Pattern.compile("\\{([a-zA-Z0-9_-]+)\\}");
	private final List<Object> m_stringParts = new LinkedList<>();
	
	// TODO need a way to escape the curly brace
	
	public StringWithReplacements(String s) {
		final int len = s.length();
		final Matcher m = REPLACEMENT_VALUE.matcher(s);
		int findStart = 0;
		while (findStart < len) {
			if (!m.find(findStart)) {
				addPart(s, findStart, len);
				break;
			}
			addPart(s, findStart, m.start());
			m_stringParts.add(new ValueNeedingReplacement(m.group(1)));
			findStart = m.end();
		}
	}
	
	public String buildString(Map<String, String> replacements) {
		final StringBuilder sb = new StringBuilder();
		for(Object o : m_stringParts) {
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
	
	public String buildString(ISupplyReplacement replacementSupplier) {
		final StringBuilder sb = new StringBuilder();
		for(Object o : m_stringParts) {
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

	private void addPart(String s, int startIndex, int endIndex) {
		int segLen = endIndex - startIndex;
		if (segLen > 0) {
			m_stringParts.add( s.substring(startIndex, endIndex) );
		}
	}
	
	public static boolean hasOneOrMoreReplacements(String s) {
		return REPLACEMENT_VALUE.matcher(s).find();
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
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('[');
		boolean isFirst = true;
		for(Object o : m_stringParts) {
			if (!isFirst) {
				sb.append(',');
			}
			if (o instanceof String) {
				sb.append('"').append((String)o).append('"');
			} else {
				final String name = ((ValueNeedingReplacement)o).name;
				sb.append('{').append(name).append('}');
			}
			isFirst = false;
		}
		return sb.toString();
	}
}
