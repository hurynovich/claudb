package com.github.tonivade.claudb.command.server;

import com.github.tonivade.claudb.command.DBCommand;
import com.github.tonivade.claudb.data.Database;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.Request;
import com.github.tonivade.resp.protocol.RedisToken;

import static com.github.tonivade.resp.protocol.RedisToken.error;
import static com.github.tonivade.resp.protocol.RedisToken.responseOk;

/**
 * @implNote This stub implementation was added because the `Redis inside` client doesn't work without this command.
 */
@Command("auth")
public class AuthCommand implements DBCommand {

  @Override
  public RedisToken execute(Database db, Request request) {
    if (request.getLength() < 1) {
      return error("ERR wrong number of arguments for 'AUTH' command.");
    }
    String password = (request.getLength() == 1) ? request.getParam(0).toString() : request.getParam(1).toString();

    if(password.isEmpty()) {
      return responseOk();
    } else {
      return error("Password based authentication is not implemented. Only empty password supported.");
    }
  }
}
