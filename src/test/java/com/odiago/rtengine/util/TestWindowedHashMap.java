// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.util;

import java.util.List;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

import com.cloudera.util.Pair;

/**
 * Test that the WindowedHashMap provides the operations we expect.
 */
public class TestWindowedHashMap {
  private <T> void assertContains(T obj, Iterable<T> things) {
    for (T thing : things) {
      if (obj.equals(thing)) {
        return; // Found it.
      }
    }

    throw new AssertionError("Could not find " + obj + " in " + things);
  }

  @Test
  public void testMap() {
    // Map with string keys, integer values, and long timestamps.
    WindowedHashMap<String, Integer, Long> map = new WindowedHashMap<String, Integer, Long>();

    // Put an entry in the map.
    map.put("foo", Integer.valueOf(42), Long.valueOf(312));

    // Verify that we can retrieve it by unrefined key.
    List<Pair<Long, Integer>> vals = map.get("foo");
    assertNotNull(vals);
    assertEquals(1, vals.size());
    assertEquals(Integer.valueOf(42), vals.get(0).getRight());
    assertEquals(Long.valueOf(312), vals.get(0).getLeft());

    // Verify that we can retrieve it by key in time range.
    List<Integer> refined = map.getRange("foo", Long.valueOf(300), Long.valueOf(400),
        true, false);
    assertNotNull(refined);
    assertEquals(1, refined.size());
    assertEquals(Integer.valueOf(42), refined.get(0));

    // Verify that time ranges before, after, and immediately following it do
    // not include the value.
    refined = map.getRange("foo", Long.valueOf(200), Long.valueOf(300), true, false);
    assertEquals(0, refined.size());
    refined = map.getRange("foo", Long.valueOf(500), Long.valueOf(600), true, false);
    assertEquals(0, refined.size());

    // Present with closed hi interval end.
    refined = map.getRange("foo", Long.valueOf(300), Long.valueOf(312), true, false);
    assertEquals(1, refined.size());

    // Open hi interval end.
    refined = map.getRange("foo", Long.valueOf(300), Long.valueOf(312), true, true);
    assertEquals(0, refined.size());

    // The lo side of the interval should return a value when closed:
    refined = map.getRange("foo", Long.valueOf(312), Long.valueOf(315), false, false);
    assertEquals(1, refined.size());
    assertEquals(Integer.valueOf(42), refined.get(0));

    // But not when open:
    refined = map.getRange("foo", Long.valueOf(312), Long.valueOf(315), true, false);
    assertEquals(0, refined.size());

    // Verify that the correct time range but the wrong key does not include
    // it.
    refined = map.getRange("bar", Long.valueOf(300), Long.valueOf(400), true, false);
    assertEquals(0, refined.size());

    // Add another key with a new timestamp.
    map.put("bar", Integer.valueOf(43), Long.valueOf(611));
    // Verify that we can retrieve it.
    refined = map.getRange("bar", Long.valueOf(600), Long.valueOf(700), true, false);
    assertEquals(1, refined.size());
    assertEquals(Integer.valueOf(43), refined.get(0));

    // Add another key with the first timestamp.
    map.put("baz", Integer.valueOf(44), Long.valueOf(312));
    // Looking up values by timestamp should return both.
    List<Integer> valsForT = map.getTimestamp(Long.valueOf(312));
    assertNotNull(valsForT);
    assertEquals(2, valsForT.size());
    assertContains(Integer.valueOf(42), valsForT);
    assertContains(Integer.valueOf(44), valsForT);

    // Looking up 'foo' should still work.
    refined = map.getRange("foo", Long.valueOf(300), Long.valueOf(400), true, false);
    assertNotNull(refined);
    assertEquals(1, refined.size());
    assertEquals(Integer.valueOf(42), refined.get(0));

    // Remove everything older than 312.
    map.removeOlderThan(Long.valueOf(312));
    // Verify that our original foo entry is still there.
    refined = map.getRange("foo", Long.valueOf(300), Long.valueOf(400), true, false);
    assertNotNull(refined);
    assertEquals(1, refined.size());
    assertEquals(Integer.valueOf(42), refined.get(0));

    // Remove the older entries by expiring them.
    map.removeOlderThan(Long.valueOf(500));
    // Try to look it up again, it should be gone.
    refined = map.getRange("foo", Long.valueOf(300), Long.valueOf(400), true, false);
    assertNotNull(refined);
    assertEquals(0, refined.size());

    refined = map.getRange("baz", Long.valueOf(300), Long.valueOf(400), true, false);
    assertNotNull(refined);
    assertEquals(0, refined.size());

    // Now remove "bar" and assert that everything is empty.
    WindowedHashMap<String, Integer, Long> emptyMap =
        new WindowedHashMap<String, Integer, Long>();
    map.remove("bar");
    assertEquals(emptyMap, map);

    // Put three entries for 'foo' in at different times, retrieve them in
    // various ways.
    map.put("foo", Integer.valueOf(1), Long.valueOf(1000));
    map.put("foo", Integer.valueOf(2), Long.valueOf(2000));
    map.put("foo", Integer.valueOf(3), Long.valueOf(3000));
    
    refined = map.getRange("foo", Long.valueOf(1500), Long.valueOf(3500), true, false);
    assertEquals(2, refined.size());
    assertContains(Integer.valueOf(2), refined);
    assertContains(Integer.valueOf(3), refined);
    
    refined = map.getRange("foo", Long.valueOf(500), Long.valueOf(3500), true, false);
    assertEquals(3, refined.size());
    assertContains(Integer.valueOf(1), refined);
    assertContains(Integer.valueOf(2), refined);
    assertContains(Integer.valueOf(3), refined);

    map.removeOlderThan(Long.valueOf(2200));
    refined = map.getRange("foo", Long.valueOf(500), Long.valueOf(3500), true, false);
    assertEquals(1, refined.size());
    assertContains(Integer.valueOf(3), refined);
  }
}

