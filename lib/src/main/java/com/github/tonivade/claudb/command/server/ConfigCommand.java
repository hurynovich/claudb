/*
 * Copyright (c) 2015-2025, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */

package com.github.tonivade.claudb.command.server;

import com.github.tonivade.claudb.DBServerState;
import com.github.tonivade.claudb.command.ParamsScanner;
import com.github.tonivade.claudb.glob.GlobPattern;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.command.RespCommand;
import com.github.tonivade.resp.protocol.RedisToken;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.tonivade.resp.protocol.RedisToken.*;
import static java.util.Locale.ROOT;

/**
 * See https://redis.io/docs/latest/commands/config/
 */
@Command("config")
public class ConfigCommand implements RespCommand {
  private static final String SUBCMD_GET = "get";
  private static final String SUBCMD_HELP = "help";

  private static final String PROP_DATABASES = "databases";

  @Override
  public RedisToken execute(Request request) {
    ParamsScanner params = new ParamsScanner(request);
    if (!params.hasNext()) {
      return error("Wrong number of arguments for 'CONFIG' command");
    }
    String subCmd = params.nextString().toLowerCase(ROOT);
    switch (subCmd) {
      case SUBCMD_HELP:
        return doHelp(params);
      case SUBCMD_GET:
        return doGet(params, request);
      default:
        return error(wrongParameters(subCmd));
    }
  }

  private RedisToken doHelp(ParamsScanner params) {
    params.verifyHasNoMore(wrongParameters(SUBCMD_HELP));
    return array(
      string("CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
      string("GET <pattern>"),
      string("    Return parameters matching the glob-like <pattern> and their values."),
      string("HELP"),
      string("    Prints this help.")
    );
  }

  private RedisToken doGet(ParamsScanner params, Request request) {
    GlobPattern pattern = params.nextGlob();
    params.verifyHasNoMore("wrong number of arguments for 'CONFIG' command");
    DBServerState dbManager = request.getServerContext().<DBServerState>getValue("state")
      .orElseThrow(() -> new IllegalStateException("missing server DB state"));

    List<Object> result = new ArrayList<>();
    //currently only one property supported
    if (pattern.match(PROP_DATABASES)) {
      result.add(PROP_DATABASES);
      result.add(dbManager.getDatabasesCount());
    }

    return array(
      result.stream()
        .map(obj -> obj == null ? nullString() : string(obj.toString()))
        .collect(Collectors.toList())
    );
  }

  private String wrongParameters(String subCmd) {
    return "Unknown subcommand or wrong number of arguments for '" + subCmd + "'.";
  }
}
