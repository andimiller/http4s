package org.http4s.client

import cats.data.Kleisli
import cats.effect.Effect
import org.http4s.Request
import org.http4s.websocket.Websocket
import fs2._

final case class DisposableWebsocket[F[_]](stream: Websocket[F], dispose: F[Unit]) {
  def asStream()(implicit F: Effect[F]): Stream[F, Websocket[F]] =
    Stream.bracket(F.unit)(_ => Stream.emit(stream), _ => dispose)
}

final case class WebsocketClient[F[_]](
    open: Kleisli[F, Request[F], DisposableWebsocket[F]],
    shutdown: F[Unit])
