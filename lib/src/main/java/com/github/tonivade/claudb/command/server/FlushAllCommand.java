package com.github.tonivade.claudb.command.server;

import com.github.tonivade.claudb.DBConfig;
import com.github.tonivade.claudb.DBServerState;
import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.annotation.ParamLength;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.resp.protocol.RedisToken.responseOk;

/**
 * See https://redis.io/docs/latest/commands/flushall/
 */
@Command("flushall")
@ParamLength(0)
public class FlushAllCommand implements DBCommand {
  @Override
  public RedisToken execute(Database db, Request request) {
    DBConfig config = request.getServerContext().<DBConfig>getValue("config")
      .orElseThrow(() -> new IllegalStateException("missing server config"));
    DBServerState serverState = getServerState(request.getServerContext());
    for (int i = 0; i < config.getNumDatabases(); i++) {
      serverState.getDatabase(i).clear();
    }
    return responseOk();
  }
}
