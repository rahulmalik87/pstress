#pragma once
/* Shared socket and TLS setup for every clickhouse-cpp client pstress creates.
   MUST be included after <clickhouse/client.h> and before the pstress headers
   that declare ::Column — see the note at the top of ch_verify.cpp. */
#include <clickhouse/client.h>
#include <chrono>
#include <string>

/* Settings every clickhouse-cpp client pstress creates needs: how the socket
   behaves, plus the TLS setup for --secure.

   Every client must go through this: pstress opens connections from three
   places (worker threads in ClickhouseDatabase.hpp, plus make_clients() and
   ch_verify_schema() in ch_verify.cpp), and a plaintext handshake against a
   TLS port fails as "can't receive string data: Connection reset by peer".

   socket_timeout_secs comes from --ch-socket-timeout; 0 leaves the read
   blocking forever, which is clickhouse-cpp's default. The option is read by
   the callers rather than here because this header is included before the
   pstress headers that declare Option - see the note at the top of
   ch_verify.cpp. */
inline void ch_apply_client_options(clickhouse::ClientOptions &opts,
                                    bool secure, int socket_timeout_secs) {
  /* Keepalive is what stops a worker from waiting forever on a reply that can
     no longer arrive. A query that takes minutes server-side - a lightweight
     DELETE waiting for its mutation on every replica is the usual one - leaves
     the connection with no traffic on it, and an idle flow crossing a NAT or
     load-balancer hop is dropped silently: no FIN, no RST, both ends still
     ESTABLISHED. The server's reply then goes nowhere and the read never
     returns, so a run whose --seconds expired sits in pthread_join until it is
     killed. Probing keeps the flow from going idle at all, and when the peer
     really is gone the failed probes make the read fail in ~75s. */
  opts.TcpKeepAlive(true)
      .SetTcpKeepAliveIdle(std::chrono::seconds(60))
      .SetTcpKeepAliveInterval(std::chrono::seconds(5))
      .SetTcpKeepAliveCount(3);

  /* Backstop for the case keepalive cannot see: a server that keeps the
     connection healthy but stops answering. */
  if (socket_timeout_secs > 0) {
    opts.SetConnectionRecvTimeout(std::chrono::seconds(socket_timeout_secs));
    opts.SetConnectionSendTimeout(std::chrono::seconds(socket_timeout_secs));
  }

  /* The SSLOptions defaults verify the server certificate against the system CA
     store and send SNI, which is what ClickHouse Cloud requires. SetSSLOptions
     throws if the linked clickhouse-cpp was built without -DWITH_OPENSSL=ON. */
  if (!secure)
    return;
  opts.SetSSLOptions(clickhouse::ClientOptions::SSLOptions());
  /* A Cloud service resuming from idle needs longer than the 5s default. */
  opts.SetConnectionConnectTimeout(std::chrono::seconds(30));
}

/* Native-protocol default port: 9440 is TLS, 9000 plaintext. */
inline int ch_default_port(bool secure) { return secure ? 9440 : 9000; }

/* Suffix for connection-failure messages, suggesting --secure where it is the
   likely cause. A plaintext handshake against a TLS port is dropped rather
   than refused, so the underlying error is "Connection timed out" or
   "Connection reset by peer" — which reads like a network or DNS fault instead
   of a missing flag. Returns "" when --secure is already set, or when the
   target is loopback (there the honest explanation is usually "no server"). */
inline std::string ch_connect_hint(const std::string &host, int port,
                                   bool secure) {
  if (secure)
    return "";
  const bool cloud = host.find(".clickhouse.cloud") != std::string::npos;
  if (cloud || port == 9440)
    return std::string("\n       hint: --secure was not passed, so this was a "
                       "plaintext connection to port ") +
           std::to_string(port) + ". " +
           (cloud ? "ClickHouse Cloud only serves the native protocol over "
                    "TLS on port 9440."
                  : "Port 9440 is the TLS port.") +
           " Retry with --secure.";
  if (host == "127.0.0.1" || host == "localhost" || host == "::1")
    return "";
  return "\n       hint: if this endpoint requires TLS, pass --secure — "
         "without it pstress connects in plaintext on port 9000.";
}
