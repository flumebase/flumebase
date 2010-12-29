// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.Pair;

/**
 * A hash map implementation suitable for windowed join operations.  Each
 * entry in the HashMap has, in addition to a key and a value, a
 * <i>timestamp</i> field with type T.
 *
 * <p>Entries are kept in a (k, List (t, v)) HashMap internally. There is a
 * second TreeMap contained inside which maps (timestamp, List k). This allows
 * for time-ranged iteration over the entries in the map, as well as insertion
 * of timestamped entries out of order (which java.util.LinkedHashMap does not
 * provide).  </p>
 *
 * <p>Multiple keys may have the same timestamp.</p>
 * <p>This map may not store null values.</p>
 */
public class WindowedHashMap<K, V, T extends Comparable<T>>
    implements Map<K, List<Pair<T, V>>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      WindowedHashMap.class.getName());

  private HashMap<K, List<Pair<T, V>>> mHashMap;
  private TreeMap<T, List<K>> mTimestamps;

  public WindowedHashMap() {
    mHashMap = new HashMap<K, List<Pair<T, V>>>();
    mTimestamps = new TreeMap<T, List<K>>();
  }

  @Override
  public int size() {
    return mHashMap.size();
  }

  @Override
  public boolean isEmpty() {
    return mHashMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return mHashMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return mHashMap.containsValue(value);
  }

  @Override
  public List<Pair<T, V>> get(Object key) {
    return mHashMap.get(key);
  }

  /**
   * @return all values for the specified timestamp.
   */
  public List<V> getTimestamp(T timestamp) {
    List<K> keysForTs = mTimestamps.get(timestamp);
    if (null == keysForTs) {
      return Collections.emptyList();
    }

    List<V> out = new ArrayList<V>();
    for (K k : keysForTs) {
      List<Pair<T, V>> timesAndValsForKey = mHashMap.get(k);
      assert(timesAndValsForKey != null);
      for (Pair<T, V> pr : timesAndValsForKey) {
        out.add(pr.getRight());
      }
    }

    return out;
  }

  /**
   * Allows a multi-put of (timestamp, value) pairs with the same key.
   */
  @Override
  public List<Pair<T, V>> put(K key, List<Pair<T, V>> timesAndVals) {
    List<Pair<T, V>> old = mHashMap.get(key);
    for (Pair<T, V> pr : timesAndVals) {
      put(key, pr.getRight(), pr.getLeft());
    }

    return old;
  }

  public void put(K key, V value, T timestamp) {
    Pair<T, V> timeVal = new Pair<T, V>(timestamp, value);
    List<Pair<T, V>> lstForKey = mHashMap.get(key);
    if (null == lstForKey) {
      lstForKey = new ArrayList<Pair<T, V>>();
      mHashMap.put(key, lstForKey);
    }

    lstForKey.add(timeVal);

    List<K> lstForT = mTimestamps.get(timestamp);
    if (null == lstForT) {
      lstForT = new ArrayList<K>();
      mTimestamps.put(timestamp, lstForT);
    }
    lstForT.add(key);
  }

  /**
   * Removes the specified key from the map.
   */
  @Override
  public List<Pair<T, V>> remove(Object key) {
    List<Pair<T, V>> timesAndVals = mHashMap.remove(key);
    if (null == timesAndVals) {
      // If we couldn't find this key, break early.
      return null;
    }

    // Remove all elements from the timestamp-driven map.
    for (Pair<T, V> tv : timesAndVals) {
      T t = tv.getLeft();
      List<K> keysForTs = mTimestamps.get(t);
      Iterator<K> it = keysForTs.iterator();
      while (it.hasNext()) {
        K someKey = it.next();
        if (someKey.equals(key)) {
          it.remove();
        }
      }

      if (keysForTs.size() == 0) {
        // Only key for the timestamp; remove the timestamp from the
        // mapping entirely.
        mTimestamps.remove(t);
      }
    }

    return timesAndVals;
  }

  @Override
  public void putAll(Map<? extends K, ? extends List<Pair<T, V>>> m) {
    for (Map.Entry<? extends K, ? extends List<Pair<T, V>>> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    mTimestamps.clear();
    mHashMap.clear();
  }

  @Override
  public Set<K> keySet() {
    return mHashMap.keySet();
  }

  @Override
  public Collection<List<Pair<T, V>>> values() {
    return mHashMap.values();
  }

  @Override
  public Set<Map.Entry<K, List<Pair<T, V>>>> entrySet() {
    return mHashMap.entrySet();
  }

  @Override
  public boolean equals(Object otherObj) {
    if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }

    WindowedHashMap<K, V, T> other = (WindowedHashMap<K, V, T>) otherObj;
    return mHashMap.equals(other.mHashMap) && mTimestamps.equals(other.mTimestamps);
  }

  @Override
  public int hashCode() {
    return mHashMap.hashCode() ^ mTimestamps.hashCode();
  }

  /**
   * Look up all values for a key within a given timestamp range. 
   * @returns a list of values such that m[key, t] = v and t is in the interval
   * bounded by lo and hi. Arguments specify whether the lower and upper bounds
   * of the interval are open-ended or closed.
   * Returns the empty list if no such key can be found within that window.
   */
  public List<V> getRange(K key, T lo, T hi, boolean openLo, boolean openHi) {
    List<Pair<T, V>> timesAndVals = mHashMap.get(key);
    
    boolean first = true;
    List<V> out = null;

    if (null == timesAndVals) {
      // No times/vals for that key.
      return Collections.emptyList();
    }

    for (Pair<T, V> pr : timesAndVals) {
      if (timeInRange(lo, pr.getLeft(), hi, openLo, openHi)) {
        if (null == out) {
          // Start by using a singleton list to hold this object. 
          out = Collections.singletonList(pr.getRight());
        } else if (first) {
          // Promote to a linked list
          out = new LinkedList<V>(out);
          out.add(pr.getRight());
          first = false;
        } else {
          // We're already in a linked list; just add it.
          out.add(pr.getRight());
        }
      }
    }

    if (null == out) {
      return Collections.emptyList();
    }

    return out;
  }

  /**
   * @return true if test is in the interval between lo and hi;
   * both endpoints can be open or closed.
   */
  private boolean timeInRange(T lo, T test, T hi, boolean openLo, boolean openHi) {
    int loVsTest = lo.compareTo(test);
    if (!(openLo ? loVsTest < 0 : loVsTest <= 0)) {
      return false; // 'test' is not above the lo end of the interval.
    }

    int hiVsTest = test.compareTo(hi);
    if (!(openHi ? hiVsTest < 0 : hiVsTest <= 0)) {
      return false; // 'test' is not below the hi end of the interval.
    }

    return true;
  }

  /**
   * Remove all (k, v) pairs where the timestamp for the entry is less than
   * the value specified by 'test'.
   */
  public void removeOlderThan(T test) {
    // Get a view of all timestamps in the map less than 'test'.
    Map<T, List<K>> subMap = mTimestamps.headMap(test, false);

    LOG.debug("Remove older than: " + test);
    Set<Map.Entry<T, List<K>>> entrySet = subMap.entrySet();
    Iterator<Map.Entry<T, List<K>>> entries = entrySet.iterator();
    while (entries.hasNext()) {
      Map.Entry<T, List<K>> entry = entries.next();
      T removeTime = entry.getKey();
      List<K> removeKeys = entry.getValue();

      // Remove all (t, v) pairs from mHashMap(k) for all k in removeKeys,
      // where t = removeTime. If the list at mHashMap(k) is empty, remove
      // mHashMap(k).
      for (K removeKey : removeKeys) {
        List<Pair<T, V>> pairs = mHashMap.get(removeKey);
        Iterator<Pair<T, V>> pairIter = pairs.iterator();
        while (pairIter.hasNext()) {
          Pair<T, V> pr = pairIter.next();
          if (pr.getLeft().equals(removeTime)) {
            LOG.debug("Removing: " + removeKey + " (t=" + removeTime + ")");
            pairIter.remove();
          }
        }

        if (pairs.size() == 0) {
          // Remove the entire key/list from mHashMap.
          mHashMap.remove(removeKey);
        }
      }

      // Remove the entry for this timestamp from the underlying map.
      entries.remove();
    }
  }

  /**
   * @return the oldest timestamp in the map, or null if the map is empty.
   */
  public T oldestTimestamp() {
    if (mTimestamps.isEmpty()) {
      return null;
    }

    return mTimestamps.firstKey();
  }
}
