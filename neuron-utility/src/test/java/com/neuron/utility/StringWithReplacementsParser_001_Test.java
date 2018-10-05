package com.neuron.utility;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringWithReplacementsParser_001_Test {
	
	@Test
	public void hasReplacements() {
		StringWithReplacementsParser parser = new StringWithReplacementsParser("{", "}", null, null);
		Assertions.assertFalse(parser.hasReplacements("this is a string"));
		Assertions.assertTrue(parser.hasReplacements("this is \\{+%\\} a string"));
		Assertions.assertTrue(parser.hasReplacements("this is \\{ a \\} a string"));
		
		Assertions.assertTrue(parser.hasReplacements("this is {a} a string"));
		Assertions.assertTrue(parser.hasReplacements("{a}this is a string"));
		Assertions.assertTrue(parser.hasReplacements("this is a string{a}"));
		
		StringWithReplacementsParser parser2 = new StringWithReplacementsParser("{", "}", "\\{", "\\}");
		Assertions.assertFalse(parser2.hasReplacements("this is a string"));
		Assertions.assertFalse(parser2.hasReplacements("this is \\{+%\\} a string"));
		Assertions.assertFalse(parser2.hasReplacements("this is \\{ a \\} a string"));
		
		Assertions.assertTrue(parser2.hasReplacements("this is \\{asdf\\} {a} a string"));
		Assertions.assertTrue(parser2.hasReplacements("this is {a} a string"));
		Assertions.assertTrue(parser2.hasReplacements("{a}this is a string"));
		Assertions.assertTrue(parser2.hasReplacements("this is a string{a}"));
		
		StringWithReplacementsParser parser3 = new StringWithReplacementsParser("***", "&&&", null, null);
		Assertions.assertFalse(parser3.hasReplacements("this is a string"));
		Assertions.assertFalse(parser3.hasReplacements("this is \\{+%\\} a string"));
		Assertions.assertFalse(parser3.hasReplacements("this is \\{ a \\} a string"));
		
		Assertions.assertTrue(parser3.hasReplacements("this is ***a&&& a string"));
		Assertions.assertTrue(parser3.hasReplacements("***a&&&this is a string"));
		Assertions.assertTrue(parser3.hasReplacements("this is a string ***a&&&"));
	}
	
	@Test
	public void test1() {
		StringWithReplacementsParser parser = new StringWithReplacementsParser("{{", "}}", null, null);
		
		System.out.println("swr=" + parser.dump_parse("This is {{a}} test!"));
		Assertions.assertEquals("This is a test!", parser.replace("This is {{a}} test!", new StringWithReplacementsParser.ISupplyReplacement() {
			public String get(String name) {
				return "a";
			}
		}));
	}
	
	@Test
	public void test2() {
		final StringWithReplacementsParser parser = new StringWithReplacementsParser("{{", "}}", null, null);
		
		final Map<String,String> map = new HashMap<String,String>();
		map.put("a", "a");
		map.put("b", "b");
		map.put("c", "c");

		Assertions.assertEquals("abc", parser.replace("{{a}}{{b}}{{c}}", map));
	}
	
	@Test
	public void test3() {
		final StringWithReplacementsParser parser = new StringWithReplacementsParser("{{", "}}", null, null);
		
		final Map<String,String> map = new HashMap<String,String>();
		map.put("a", "a");
		map.put("c", "c");
		map.put("e", "e");

		Assertions.assertEquals("abcde", parser.replace("{{a}}b{{c}}d{{e}}", map));
	}
	
	@Test
	public void test4() {
		final StringWithReplacementsParser parser = new StringWithReplacementsParser("{", "}", null, null);
		
		final Map<String,String> map = new HashMap<String,String>();
		map.put("b", "b");
		map.put("d", "d");
		map.put("f", "f");

		Assertions.assertEquals("abcdefg", parser.replace("a{b}c{d}e{f}g", map));
	}
	
	@Test
	public void test5() {
		final StringWithReplacementsParser parser = new StringWithReplacementsParser("{", "}", null, null);

		try {
			parser.replace("a{}b", new HashMap<String,String>());
			Assertions.fail("Did not get an exception");
		} catch(RuntimeException ex) {
			Assertions.assertEquals("String has an empty open/close pair at index 3", ex.getMessage());
		}
	}
}
