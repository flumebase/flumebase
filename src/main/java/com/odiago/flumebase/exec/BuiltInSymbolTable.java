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

package com.odiago.flumebase.exec;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.builtins.*;

import com.odiago.flumebase.lang.Function;
import com.odiago.flumebase.lang.Type;

/**
 * A symbol table that is pre-populated with builtin functions
 * and other symbols for the user to avail himself of.
 */
public class BuiltInSymbolTable extends SymbolTable {
  private static final Logger LOG = LoggerFactory.getLogger(
      BuiltInSymbolTable.class.getName());

  private static Map<String, Symbol> BUILTINS;
  static {
    BUILTINS = new TreeMap<String, Symbol>();
    // Add symbols for all built-in objects in the system.
    loadBuiltinFunction(avg.class);
    loadBuiltinFunction(bin2str.class);
    loadBuiltinFunction(concat.class);
    loadBuiltinFunction(contains.class);
    loadBuiltinFunction(count.class);
    loadBuiltinFunction(current_timestamp.class);
    loadBuiltinFunction(event_timestamp.class);
    loadBuiltinFunction(host.class);
    loadBuiltinFunction(index.class);
    loadBuiltinFunction(length.class);
    loadBuiltinFunction(min.class);
    loadBuiltinFunction(max.class);
    loadBuiltinFunction(priority.class);
    loadBuiltinFunction(priority_level.class);
    loadBuiltinFunction(size.class);
    loadBuiltinFunction(square.class);
    loadBuiltinFunction(str2bin.class);
    loadBuiltinFunction(sum.class);
    loadBuiltinFunction(to_list.class);
    BUILTINS = Collections.unmodifiableMap(BUILTINS);
  }

  public BuiltInSymbolTable() {
  }

  @Override
  public SymbolTable getParent() {
    // This symbol table never has a parent.
    return null;
  }

  @Override
  public Symbol resolve(String symName) {
    return resolveLocal(symName);
  }

  @Override
  public Symbol resolveLocal(String symName) {
    return BUILTINS.get(symName);
  }

  @Override
  public void addSymbol(Symbol sym) {
    throw new RuntimeException("Cannot add symbols to built in symbol table.");
  }

  @Override
  public void remove(String symName) {
    throw new RuntimeException("Cannot add symbols to built in symbol table.");
  }

  @Override
  public Iterator<Symbol> iterator() {
    return levelIterator();
  }

  @Override
  public Iterator<Symbol> levelIterator() {
    return BUILTINS.values().iterator();
  }

  /**
   * Load instances of the built-in functions into the BuiltInSymbolTable.
   */
  private static void loadBuiltinFunction(Class<? extends Function> cls) {
    try {
      Function fn = (Function) cls.newInstance();
      Type retType = fn.getReturnType();
      List<Type> argTypes = fn.getArgumentTypes();
      List<Type> varArgTypes = fn.getVarArgTypes();
      String fnName = cls.getSimpleName();
      LOG.debug("Loaded built-in function: " + fnName);
      Symbol fnSymbol = new FnSymbol(fnName, fn, retType, argTypes, varArgTypes);
      BUILTINS.put(fnName, fnSymbol);
    } catch (InstantiationException ie) {
      LOG.error("Could not instantiate class: " + ie);
    } catch (IllegalAccessException iae) {
      LOG.error("IllegalAccessException creating class instance: " + iae);
    }
  }
}
