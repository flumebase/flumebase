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

package com.odiago.flumebase.exec.builtins;

import java.nio.ByteBuffer;

import java.nio.charset.Charset;

import java.util.Collections;
import java.util.List;

import org.apache.avro.util.Utf8;

import com.odiago.flumebase.exec.EventWrapper;

import com.odiago.flumebase.lang.ScalarFunc;
import com.odiago.flumebase.lang.Type;

/**
 * Return the STRING representation of a BINARY object.
 */
public class bin2str extends ScalarFunc {
  /** Charset used to decode all ByteBuffers */
  private static Charset UTF8_CHARSET;

  static {
    UTF8_CHARSET = Charset.forName("UTF-8");
  }

  @Override
  public Type getReturnType() {
    return Type.getNullable(Type.TypeName.STRING);
  }

  @Override
  public Object eval(EventWrapper event, Object... args) {
    Object arg0 = args[0];
    if (null == arg0) {
      return null;
    } else {
      ByteBuffer bytes = (ByteBuffer) arg0;
      return new Utf8(new String(bytes.array(), bytes.position(), bytes.remaining(),
            UTF8_CHARSET));
    }
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.singletonList(Type.getNullable(Type.TypeName.BINARY));
  }
}
