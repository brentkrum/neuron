package com.neuron.utility;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.neuron.utility.StringWithReplacements.ISupplyReplacement;

public class StringWithReplacements_001_Test {
	
	@Test
	public void hasOneOrMoreReplacements() {
		Assertions.assertFalse(StringWithReplacements.hasOneOrMoreReplacements("this is a string"));
		Assertions.assertFalse(StringWithReplacements.hasOneOrMoreReplacements("this is {+%} a string"));
		Assertions.assertFalse(StringWithReplacements.hasOneOrMoreReplacements("this is { a } a string"));
		
		Assertions.assertTrue(StringWithReplacements.hasOneOrMoreReplacements("this is {a} a string"));
		Assertions.assertTrue(StringWithReplacements.hasOneOrMoreReplacements("{a}this is a string"));
		Assertions.assertTrue(StringWithReplacements.hasOneOrMoreReplacements("this is a string{a}"));
	}
	
	@Test
	public void test1() {
		StringWithReplacements swr = new StringWithReplacements("This is {a} test!");
		System.out.println("swr=" + swr);
		String out = swr.buildString(new ISupplyReplacement() {
			public String get(String name) {
				return "a";
			}
		});
		Assertions.assertEquals("This is a test!", out);
	}
	
	@Test
	public void test2() {
		final Map<String,String> map = new HashMap<String,String>();
		map.put("a", "a");
		map.put("b", "b");
		map.put("c", "c");

		StringWithReplacements swr = new StringWithReplacements("{a}{b}{c}");
		String out = swr.buildString(map);
		Assertions.assertEquals("abc", out);
	}
	
	@Test
	public void test3() {
		final Map<String,String> map = new HashMap<String,String>();
		map.put("a", "a");
		map.put("c", "c");
		map.put("e", "e");

		StringWithReplacements swr = new StringWithReplacements("{a}b{c}d{e}");
		String out = swr.buildString(map);
		Assertions.assertEquals("abcde", out);
	}
	
	@Test
	public void test4() {
		final Map<String,String> map = new HashMap<String,String>();
		map.put("b", "b");
		map.put("d", "d");
		map.put("f", "f");

		StringWithReplacements swr = new StringWithReplacements("a{b}c{d}e{f}g");
		String out = swr.buildString(map);
		Assertions.assertEquals("abcdefg", out);
	}
}
