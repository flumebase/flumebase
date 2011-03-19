// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.testutil;

import java.io.IOException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.odiago.flumebase.exec.BuiltInSymbolTable;
import com.odiago.flumebase.exec.FlowId;
import com.odiago.flumebase.exec.HashSymbolTable;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.exec.local.LocalEnvironment;
import com.odiago.flumebase.exec.local.MemoryOutputElement;

import static org.testng.AssertJUnit.*;

import com.odiago.flumebase.flume.EmbeddedFlumeConfig;

/**
 * Base class for tests that connect to a LocalEnvironment and execute RTSQL
 * queries against in-memory streams.
 */
public class RtsqlTestCase {

  private LocalEnvironment mEnvironment;
  private SymbolTable mSymbolTable;
  private Configuration mConf;
  private Map<String, MemoryOutputElement> mOutputs;
  private EmbeddedFlumeConfig mFlumeConfig;

  @BeforeMethod
  public void setUp() {
    mSymbolTable = new HashSymbolTable(new BuiltInSymbolTable());
    mConf = new Configuration();
    mOutputs = Collections.synchronizedMap(new HashMap<String, MemoryOutputElement>());
    mFlumeConfig = new EmbeddedFlumeConfig(mConf);
    mEnvironment = new LocalEnvironment(mConf, mSymbolTable, mOutputs, mFlumeConfig);
  }

  @BeforeMethod(groups = { "slow" })
  public void slowSetUp() throws IOException, InterruptedException {
    setUp();
  }

  @AfterMethod
  public void tearDown() throws IOException, InterruptedException {
    if (null != mEnvironment && mEnvironment.isConnected()) {
      mEnvironment.shutdown();
    }

    if (mFlumeConfig.isRunning()) {
      mFlumeConfig.stop();
    }
  }

  @AfterMethod(groups = { "slow" })
  public void slowTearDown() throws IOException, InterruptedException {
    tearDown();
  }

  protected Map<String, String> getQueryOpts() {
    // TODO: Pull "flumebase.*" from mConf.
    return new HashMap<String, String>();
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

  protected EmbeddedFlumeConfig getFlumeConfig() {
    return mFlumeConfig;
  }

  protected Map<String, MemoryOutputElement> getOutputs() {
    return mOutputs;
  }

  protected MemoryOutputElement getOutput(String outputName) {
    return mOutputs.get(outputName);
  }

  /**
   * Waits for a flow to complete. If this does not happen within 60 seconds,
   * a timeout occurs and the test fails.
   */
  protected void joinFlow(FlowId id) throws IOException, InterruptedException {
    boolean finished = mEnvironment.joinFlow(id, 60000);
    if (!finished) {
      fail("Waited > 60s for flow to complete -- flow executor is stuck?");
    }
  }

  /**
   * Asserts that within a set of records, there is no record such that record[fieldName] == val.
   * Note that if multiple such asserts are to be made, it may be necessary to synchronize
   * on the list to ensure it is consistent throughout multiple asserts.
   */
  protected void assertNoSuchRecord(List<GenericData.Record> records, String fieldName,
      Object val) {
    for (GenericData.Record record : records) {
      if (null == val) {
        if (record.get(fieldName) == null) {
          fail("Assertion failed: Found record with null in field " + fieldName);
        }
      } else if (val.equals(record.get(fieldName))) {
        fail("Assertion failed: Found record with field " + fieldName + " equals " + val);
      }
    }
  }

  /**
   * Asserts that for all records in 'records' such that the value of referenceField
   * is referenceValue, testField has value testValue.
   */
  protected void assertRecordFields(List<GenericData.Record> records,
      String referenceField, Object referenceValue,
      String testField, Object testValue) {
    for (GenericData.Record record : records) {
      if (null == referenceValue) {
        if (null == record.get(referenceField)) {
          checkField(record, testField, testValue);
        }
      } else if (referenceValue.equals(record.get(referenceField))) {
        checkField(record, testField, testValue);
      }
    }
  }

  /**
   * Asserts that there exists a record in records such that testField = testValue.
   */
  protected void assertRecordExists(List<GenericData.Record> records,
      String testField, Object testValue) {
    for (GenericData.Record record : records) {
      Object result = record.get(testField);
      if (null == testValue && null == result) {
        return;
      } else if (null != testValue && testValue.equals(result)) {
        return;
      }
    }

    fail("Could not find record such that " + testField + " equals " + testValue);
  }

  private void checkField(GenericData.Record record, String testField, Object testValue) {
    if (null == testValue) {
      if (record.get(testField) != null) {
        fail("Assertion failed: candidate record had non-null value for " + testField);
      }
    } else if (!testValue.equals(record.get(testField))) {
      fail("Assertion failed: test field had value " + record.get(testField)
          + ", but expected " + testValue);
    }
  }

  // mvn surefire test runner complains if this does not contain at least one test.
  @Test
  public void ignoredTestCase() { }
}
