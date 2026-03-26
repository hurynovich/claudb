/*
 * Copyright (c) 2015-2025, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.key;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.ParamsScanner;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseKey;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.claudb.glob.GlobPattern;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static com.github.tonivade.resp.protocol.RedisToken.*;
import static java.util.stream.Collectors.toSet;

@ReadOnly
@Command("scan")
//TODO: this is naive implementation and performance can be improved
//TODO: common superclass can be extracted with KeysComman
public class ScanCommand implements DBCommand {
  public static final int DEFAULT_COUNT = 100;
  public static final String DEFAULT_PATTERN = "*";

  @Override
  public RedisToken execute(Database db, Request request) {
    ParamsScanner parser = new ParamsScanner(request);
    int cursor = parser.nextInt();
    GlobPattern pattern = parser.nextGlob("match", DEFAULT_PATTERN);
    int count = parser.nextInt("count", DEFAULT_COUNT);

    Set<RedisToken> keys = db.entrySet().stream()
      .filter(matchPattern(pattern))
      .filter(filterExpired(Instant.now()).negate())
      .map(Map.Entry::getKey)
      .map(DatabaseKey::getValue)
      .skip(cursor)
      .limit(count)
      .map(RedisToken::string)
      .collect(toSet());

    return array(
      string(String.valueOf((count > keys.size()) ? 0 : cursor + count)),
      array(keys)
    );
  }

  private Predicate<Map.Entry<DatabaseKey, DatabaseValue>> filterExpired(Instant now) {
    return entry -> entry.getValue().isExpired(now);
  }

  private Predicate<Map.Entry<DatabaseKey, DatabaseValue>> matchPattern(GlobPattern pattern) {
    return entry -> pattern.match(entry.getKey().toString());
  }
}
