package org.http4s

import fs2._
import org.http4s.websocket.WebSocketFrame

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Cannot convert from WebSocketFrames to ${A}, because no WebSocketDecoder[${F}, ${A}] instance could be found.")
trait WebSocketDecoder[F[_], A] { self =>
  def decode(ws: Stream[F, WebSocketFrame]): Stream[]
}


