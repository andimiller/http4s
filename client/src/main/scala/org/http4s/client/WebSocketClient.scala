package org.http4s
package client

import cats.effect._
import fs2._

import org.http4s.websocket.WebSocketFrame

case class WebSocketConnection[F[_], I, O](
  in: Stream[F, I],
  out: Pipe[F, O, Unit],
  close: F[Unit]
)

trait WebSocketClient[F[_]] {
  def run(r: Request[F]): Resource[F, WebSocketConnection[F, WebSocketFrame]]
  def pipe(r: Request[F])(f: Pipe[F, WebSocketFrame, WebSocketFrame]): F[Unit]
  def connect(s: String): Resource[F, WebSocketConnection[F, WebSocketFrame]]
  def connect(u: Uri): Resource[F, WebSocketConnection[F, WebSocketFrame]]
}

object WebSocketClient {
  type RawWebSocketConnection[F[_]] = WebSocketConnection[F, WebSocketFrame]
  def apply[F[_]](f: Request[F] => Resource[F, RawWebSocketConnection[F]])(implicit F: Sync[F]): WebSocketClient[F] = new DefaultWebSocketClient[F]() {
    override def run(r: Request[F]): Resource[F, RawWebSocketConnection[F]] = f(r)
  }

  abstract class DefaultWebSocketClient[F[_]](implicit F: Sync[F]) extends WebSocketClient[F] {
    override def connect(s: String): Resource[F, WebSocketConnection[F, WebSocketFrame]] = run(Request(uri = Uri.unsafeFromString(s)))
    override def connect(u: Uri): Resource[F, WebSocketConnection[F, WebSocketFrame]] = run(Request(uri = u))
    override def pipe(r: Request[F])(f: Pipe[F, WebSocketFrame, WebSocketFrame]): F[Unit] = run(r).use { ws =>
      ws.receive.through(f).through(ws.send).compile.drain
    }
  }

}
