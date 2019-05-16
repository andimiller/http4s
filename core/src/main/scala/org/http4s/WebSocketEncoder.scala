package org.http4s

import cats.Contravariant
import cats.effect.IO
import org.http4s.websocket.WebSocketFrame
import fs2._
import scodec.bits.ByteVector

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Cannot convert from ${A} to WebSocketFrames, because no WebSocketEncoder[${F}, ${A}] instance could be found.")
trait WebSocketEncoder[F[_], A] { self =>
  def encode(a: A): Stream[F, WebSocketFrame]
  def contramap[B](f: B => A): WebSocketEncoder[F, B] = WebSocketEncoder { a => self.encode(f(a)) }
}

object WebSocketEncoder {
  /** Summoner */
  def apply[F[_], A](implicit E: WebSocketEncoder[F, A]): WebSocketEncoder[F, A] = E

  /** Constructor to wrap a function from A to Stream of WebSocketFrames */
  def apply[F[_], A](f: A => Stream[F, WebSocketFrame]): WebSocketEncoder[F, A] = new WebSocketEncoder[F, A] {
    override def encode(a: A): Stream[F, WebSocketFrame] = f(a)
  }


  // Instances for WebSocketEncoder
  implicit def contravariant[F[_]]: Contravariant[WebSocketEncoder[F, ?]] = new Contravariant[WebSocketEncoder[F, ?]] {
    override def contramap[A, B](fa: WebSocketEncoder[F, A])(f: B => A): WebSocketEncoder[F, B] = fa.contramap(f)
  }

  // Base instances of WebSocketEncoder
  implicit def stringWebSocketEncoder[F[_]]: WebSocketEncoder[F, String] = WebSocketEncoder { s: String =>
    Stream.emit(WebSocketFrame.Text(s, true))
  }

  implicit def byteVectorWebSocketEncoder[F[_]]: WebSocketEncoder[F, ByteVector] = WebSocketEncoder { bv: ByteVector =>
    Stream.emit(WebSocketFrame.Binary(bv, true))
  }
}
