/*
 * Copyright (c) 2015-2025, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.hash;

import static com.github.tonivade.claudb.data.DatabaseKey.safeKey;
import static com.github.tonivade.claudb.data.DatabaseValue.entry;
import static com.github.tonivade.claudb.data.DatabaseValue.hash;
import static com.github.tonivade.resp.protocol.RedisToken.integer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.tonivade.claudb.command.CommandException;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ParamType;
import com.github.tonivade.claudb.data.DataType;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

@Command("hset")
@ParamType(DataType.HASH)
public class HashSetCommand implements DBCommand {

  @Override
  public RedisToken execute(Database db, Request request) {
    if (request.getLength() < 3 || request.getLength() % 2 != 1) {
      throw new CommandException("Wrong number of arguments for 'HSET' command");
    }

    Map<SafeString, SafeString> value = new LinkedHashMap<>();
    Iterator<SafeString> params = request.getParams().iterator();
    SafeString key = params.next();
    do {
      value.put(params.next(), params.next());
    } while (params.hasNext());

    DatabaseValue resultValue = db.merge(safeKey(key), hash(value),
        (oldValue, newValue) -> {
          Map<SafeString, SafeString> merge = new HashMap<>();
          merge.putAll(oldValue.getHash());
          merge.putAll(newValue.getHash());
          return hash(merge);
        });

    Map<SafeString, SafeString> resultMap = resultValue.getHash();

    return integer(resultMap.get(request.getParam(1)) == null);
  }
}
