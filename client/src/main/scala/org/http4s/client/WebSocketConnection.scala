package org.http4s.client

import cats._
import cats.implicits._
import cats.arrow.Profunctor
import fs2._

case class WebSocketConnection[F[_], O, I](
    in: Stream[F, I],
    out: Pipe[F, O, Unit],
    close: F[Unit]
) {
  def map[I2](f: I => I2): WebSocketConnection[F, O, I2] =
    copy(in = in.map(f))

  def imapK[G[_]](fg: F ~> G, gf: G ~> F): WebSocketConnection[G, O, I] =
    WebSocketConnection(
      in.translate(fg), { s: Stream[G, O] =>
        out(s.translate(gf)).translate(fg)
      },
      fg(close)
    )

}

object WebSocketConnection {
  implicit def webSocketConnectionProfunctor[F[_]]: Profunctor[WebSocketConnection[F, ?, ?]] =
    new Profunctor[WebSocketConnection[F, ?, ?]] {
      override def dimap[A, B, C, D](fab: WebSocketConnection[F, A, B])(f: C => A)(
          g: B => D): WebSocketConnection[F, C, D] =
        fab.copy(
          fab.in.map(g), { s: Stream[F, C] =>
            s.map(f).through(fab.out)
          },
          fab.close
        )
    }
}
