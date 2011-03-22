/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.BinaryEncoder;

import static org.testng.AssertJUnit.*;
import org.testng.annotations.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

import com.odiago.flumebase.lang.Type;

public class TestAvroEventParser {
  /**
   * Make an Avro record with the given schema.
   */
  private Event makeEvent(GenericData.Record record, Schema schema) throws IOException {
    GenericDatumWriter<GenericRecord> datumWriter =
        new GenericDatumWriter<GenericRecord>(schema);
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(outBytes);

    datumWriter.write(record, encoder);
    return new EventImpl(outBytes.toByteArray());
  }

  private AvroEventParser makeParser(Schema schema) {
    Map<String, String> params = new HashMap<String, String>();
    params.put(AvroEventParser.SCHEMA_PARAM, schema.toString());
    return new AvroEventParser(params);
  }

  @Test
  public void testReadFields() throws ColumnParseException, IOException {
    // Given a schema containing an int and a string, pull both fields out of a record.
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    fields.add(new Schema.Field("left", Schema.create(Schema.Type.INT), null, null));
    fields.add(new Schema.Field("right", Schema.create(Schema.Type.STRING), null, null));
    Schema schema = Schema.createRecord("recordname", null, null, false);
    schema.setFields(fields);

    GenericData.Record record = new GenericData.Record(schema);
    record.put("left", 4);
    record.put("right", "foo");

    Event event = makeEvent(record, schema);
    EventParser parser = makeParser(schema);
    parser.reset(event);

    Integer outLeft = (Integer) parser.getColumn(0, Type.getPrimitive(Type.TypeName.INT));
    assertEquals(4, outLeft.intValue());

    CharSequence outRight = (CharSequence)
        parser.getColumn(1, Type.getPrimitive(Type.TypeName.STRING));
    assertEquals("foo", outRight.toString());
  }
}
