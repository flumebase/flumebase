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

package com.odiago.flumebase.parser;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

import com.odiago.flumebase.exec.BuiltInSymbolTable;
import com.odiago.flumebase.exec.HashSymbolTable;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.TypeChecker;

public class TestFnCallExpr extends ExprTestCase {

  @Test
  public void testLengthFn() throws Exception {
    FnCallExpr fnCallExpr;
    TypeChecker checker;
    Object value;
    SymbolTable symbols = new HashSymbolTable(new BuiltInSymbolTable());

    // This is expected to succeed.
    fnCallExpr = new FnCallExpr("length");
    fnCallExpr.addArg(new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "meep"));
    checker = new TypeChecker(symbols);
    fnCallExpr.accept(checker);
    value = fnCallExpr.eval(getEmptyEventWrapper());

    assertEquals(Integer.valueOf(4), value);
  }
}
