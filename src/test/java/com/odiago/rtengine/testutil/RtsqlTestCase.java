// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.testutil;

import java.io.IOException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;

import org.apache.hadoop.conf.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.BuiltInSymbolTable;
import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.exec.local.LocalEnvironment;
import com.odiago.rtengine.exec.local.MemoryOutputElement;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.testutil.MemStreamBuilder;

import static org.junit.Assert.*;

/**
 * Base class for tests that connect to a LocalEnvironment and execute RTSQL
 * queries against in-memory streams.
 */
public class RtsqlTestCase {

  private LocalEnvironment mEnvironment;
  private SymbolTable mSymbolTable;
  private Configuration mConf;
  private Map<String, MemoryOutputElement> mOutputs;

  @Before
  public void setUp() {
    mSymbolTable = new HashSymbolTable(new BuiltInSymbolTable());
    mConf = new Configuration();
    mOutputs = Collections.synchronizedMap(new HashMap<String, MemoryOutputElement>());
    mEnvironment = new LocalEnvironment(mConf, mSymbolTable, mOutputs);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (null != mEnvironment && mEnvironment.isConnected()) {
      mEnvironment.disconnect();
    }
  }

  protected LocalEnvironment getEnvironment() {
    return mEnvironment;
  }

  protected SymbolTable getSymbolTable() {
    return mSymbolTable;
  }

  protected Configuration getConf() {
    return mConf;
  }

  protected Map<String, MemoryOutputElement> getOutputs() {
    return mOutputs;
  }

  protected MemoryOutputElement getOutput(String outputName) {
    return mOutputs.get(outputName);
  }

  // mvn surefire test runner complains if this does not contain at least one test.
  @Test
  public void ignoredTestCase() { }
}
