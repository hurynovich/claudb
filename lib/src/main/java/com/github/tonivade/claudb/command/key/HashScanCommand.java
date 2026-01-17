/*
 * Copyright (c) 2015-2025, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package com.github.tonivade.claudb.command.key;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.ParamsParser;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.claudb.data.DatabaseKey;
import com.github.tonivade.claudb.data.DatabaseValue;
import com.github.tonivade.claudb.glob.GlobPattern;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.github.tonivade.resp.protocol.RedisToken.array;
import static com.github.tonivade.resp.protocol.RedisToken.string;
import static java.util.stream.Collectors.toList;

/**
 * HSCAN key cursor [MATCH pattern] [COUNT count]
 */
@ReadOnly
@Command("hscan")
public class HashScanCommand implements DBCommand {
  public static final int DEFAULT_COUNT = 100;
  public static final String DEFAULT_PATTERN = "*";

  @Override
  public RedisToken execute(Database db, Request request) {
    ParamsParser parser = new ParamsParser(request);
    String key = parser.nextString();
    int cursor = parser.nextInt();
    GlobPattern pattern = parser.nextGlob("match", DEFAULT_PATTERN);
    int count = parser.nextInt("count", DEFAULT_COUNT);
    parser.verifyHasNoMore();

    List<RedisToken> result = db.getHash(key).entrySet().stream()
      .filter(matchPattern(pattern))
      .skip(cursor)
      .limit(count)
      .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
      .map(RedisToken::string)
      .collect(toList());

    return array(
      string(String.valueOf((count > result.size()) ? 0 : cursor + count)),
      array(result)
    );
  }

  private Predicate<Map.Entry<DatabaseKey, DatabaseValue>> filterExpired(Instant now) {
    return entry -> entry.getValue().isExpired(now);
  }

  private Predicate<Map.Entry<SafeString, SafeString>> matchPattern(GlobPattern pattern) {
    return entry -> pattern.match(entry.getKey().toString());
  }
}
