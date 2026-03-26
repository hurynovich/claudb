package com.github.tonivade.claudb.command.server;

import com.github.tonivade.claudb.DBServerState;
import com.github.tonivade.claudb.data.OnHeapMVDatabaseFactory;
import com.github.tonivade.resp.annotation.Command;
import com.github.tonivade.resp.command.*;
import com.github.tonivade.resp.protocol.AbstractRedisToken.ArrayRedisToken;
import com.github.tonivade.resp.protocol.AbstractRedisToken.StringRedisToken;
import com.github.tonivade.resp.protocol.RedisToken;
import com.github.tonivade.resp.protocol.SafeString;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.tonivade.resp.protocol.RedisToken.string;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigCommandTest {
  ServerContext server = mock();
  Session session = mock();
  RespCommand command = new ConfigCommand();

  @Test
  public void testHelp() {
    Request request = makeRequest("help");

    ArrayRedisToken result = (ArrayRedisToken) command.execute(request);

    List<String> help = result.getValue().stream()
      .map(line -> ((StringRedisToken) line).getValue().toString())
      .collect(Collectors.toList());
    assertTrue(help.get(0).startsWith("CONFIG <subcommand>"));
    assertTrue(help.contains("GET <pattern>"));
    assertTrue(help.contains("HELP"));
  }

  @Test
  public void testGetDatabases() {
    final int numDatabases = 3;
    when(server.getValue("state"))
      .thenReturn(Optional.of(new DBServerState(new OnHeapMVDatabaseFactory(), numDatabases)));
    Request request = makeRequest("get", "databases");

    RedisToken result = command.execute(request);

    RedisToken expected = RedisToken.array(string("databases"), string(String.valueOf(numDatabases)));
    assertEquals(expected, result);
  }

  private Request makeRequest(String... params) {
    String commandName = command.getClass().getAnnotation(Command.class).value();
    return new DefaultRequest(server, session, SafeString.safeString(commandName), SafeString.safeAsList(params));
  }
}