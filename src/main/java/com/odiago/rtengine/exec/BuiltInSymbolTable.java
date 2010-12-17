// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.builtins.*;

import com.odiago.rtengine.lang.ScalarFunc;
import com.odiago.rtengine.lang.Type;

/**
 * A symbol table that is pre-populated with builtin functions
 * and other symbols for the user to avail himself of.
 */
public class BuiltInSymbolTable extends SymbolTable {
  private static final Logger LOG = LoggerFactory.getLogger(
      BuiltInSymbolTable.class.getName());

  private static Map<String, Symbol> BUILTINS;
  static {
    BUILTINS = new HashMap<String, Symbol>();
    // Add symbols for all built-in objects in the system.
    loadBuiltinFunction(current_timestamp.class);
    loadBuiltinFunction(length.class);
    loadBuiltinFunction(square.class);
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
  private static void loadBuiltinFunction(Class<? extends ScalarFunc> cls) {
    try {
      ScalarFunc scalarFn = (ScalarFunc) cls.newInstance();
      Type retType = scalarFn.getReturnType();
      List<Type> argTypes = scalarFn.getArgumentTypes();
      String fnName = cls.getSimpleName();
      LOG.debug("Loaded built-in function: " + fnName);
      Symbol fnSymbol = new FnSymbol(fnName, scalarFn, retType, argTypes);
      BUILTINS.put(fnName, fnSymbol);
    } catch (InstantiationException ie) {
      LOG.error("Could not instantiate class: " + ie);
    } catch (IllegalAccessException iae) {
      LOG.error("IllegalAccessException creating class instance: " + iae);
    }
  }
}
