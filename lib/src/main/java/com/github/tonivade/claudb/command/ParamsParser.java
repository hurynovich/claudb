package com.github.tonivade.claudb.command;

import com.github.tonivade.claudb.glob.GlobPattern;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.SafeString;

import java.util.function.Function;

//TODO: javadoc
//TODO: unit test
public final class ParamsParser {
  private final Request request;
  private int index = 0;

  public ParamsParser(Request request) {
    this.request = request;
  }

  public boolean hasNext() {
    return index < request.getLength();
  }

  public void verifyHasNext() {
    if (index >= request.getLength()) {
      throw new CommandException("Wrong number of arguments, expected more arguments.");
    }
  }

  public void verifyHasNoMore() {
    if (index < request.getLength()) {
      throw new CommandException("Wrong number of arguments, no more arguments expected.");
    }
  }

  public boolean hasNext(String name) {
    return index < request.getLength() && name.equalsIgnoreCase(request.getParam(index).toString());
  }

  public boolean isNext(String name) {
    if (request.getLength() <= index) {
      throw new CommandException("No more parameters.");
    }
    return name.equalsIgnoreCase(request.getParam(index).toString());
  }

  public int nextInt() {
    return getValue(ParamsParser::parseInt);
  }

  public int nextInt(String name) {
    return getNamedValue(name, ParamsParser::parseInt);
  }

  public int nextInt(String name, int defValue) {
    return getOptionalNamedValue(name, defValue, ParamsParser::parseInt);
  }

  public String nextString() {
    return getValue(SafeString::toString);
  }

  public String nextString(String name) {
    return getNamedValue(name, SafeString::toString);
  }

  public String nextString(String name, String defValue) {
    return getOptionalNamedValue(name, defValue, SafeString::toString);
  }

  public GlobPattern nextGlob() {
    return new GlobPattern(nextString());
  }

  public GlobPattern nextGlob(String name) {
    return new GlobPattern(nextString(name));
  }

  public GlobPattern nextGlob(String name, String defValue) {
    return new GlobPattern(nextString(name, defValue));
  }

  private <T> T getValue(Function<SafeString, T> converter) {
    T result = converter.apply(request.getParam(index));
    index++;
    return result;
  }

  private <T> T getNamedValue(String name, Function<SafeString, T> extractor) {
    if (request.getLength() <= index) {
      throw new CommandException("Expected parameter named '" + name + "' is missing.");
    }
    SafeString actualName = request.getParam(index);
    if (!name.equalsIgnoreCase(actualName.toString())) {
      throw new CommandException("Expected parameter named '" + name + "', but found '" + actualName + "'.");
    }
    if (request.getLength() <= index + 1) {
      throw new CommandException("Value for parameter '" + name + "' is missing.");
    }
    T result = extractor.apply(request.getParam(index + 1));
    index += 2;
    return result;
  }

  private <T> T getOptionalNamedValue(String name, T defValue, Function<SafeString, T> extractor) {
    if (request.getLength() <= index) {
      return defValue;
    }
    SafeString actualName = request.getParam(index);
    if (!name.equalsIgnoreCase(actualName.toString())) {
      return defValue;
    }
    if (request.getLength() <= index + 1) {
      throw new CommandException("Value for parameter '" + name + "' is missing.");
    }
    T result = extractor.apply(request.getParam(index + 1));
    index += 2;
    return result;
  }

  private static Integer parseInt(SafeString str) {
    try {
      return Integer.parseInt(str.toString());
    } catch (NumberFormatException e) {
      throw new CommandException("Value is not an integer or out of range");
    }
  }
}
