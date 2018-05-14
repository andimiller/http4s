package org.http4s.client

import cats.MonadError
import cats.data.Kleisli
import org.http4s.Request
import org.http4s.websocket.Websocket

final case class DisposableWebsocket[F](stream: Websocket[F], dispose: F[Unit])

final case class WebsocketClient[F[_]](
    open: Kleisli[F, Request[F], DisposableWebsocket[F]],
    shutdown: F[Unit])(implicit F: MonadError[F, Throwable]) {



}
