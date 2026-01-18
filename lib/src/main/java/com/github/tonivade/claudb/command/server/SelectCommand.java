/*
 * Copyright (c) 2015-2025, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */

package com.github.tonivade.claudb.command.server;

import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.responseOk;

import com.github.tonivade.claudb.DBConfig;
import com.github.tonivade.claudb.command.ParamsScanner;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.command.annotation.ReadOnly;
import com.github.tonivade.claudb.data.Database;

@ReadOnly
@Command("select")
@ParamLength(1)
public class SelectCommand implements DBCommand {

  @Override
  public RedisToken execute(Database db, Request request) {
    int dbIndex = new ParamsScanner(request).nextInt();
    DBConfig config = getDbConfig(request.getServerContext());
    if (config.getNumDatabases() <= dbIndex) {
      return error("ERR DB index is out of range");
    }
    getSessionState(request.getSession()).setCurrentDB(dbIndex);
    return responseOk();
  }
}
