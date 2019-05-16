package org.http4s
package client

import cats._
import cats.implicits._
import cats.arrow.Profunctor
import cats.data.Chain
import cats.effect._
import fs2._
import org.http4s.client.WebSocketClient.RawWebSocketConnection
import java.nio.charset.StandardCharsets.UTF_8

import org.http4s.websocket.WebSocketFrame

trait WebSocketClient[F[_]] {
  def run(r: Request[F]): Resource[F, RawWebSocketConnection[F]]
  def pipe(r: Request[F])(f: Pipe[F, WebSocketFrame, WebSocketFrame]): F[Unit]
  def connect(s: String): Resource[F, RawWebSocketConnection[F]]
  def connect(u: Uri): Resource[F, RawWebSocketConnection[F]]
}

object WebSocketClient {
  type RawWebSocketConnection[F[_]] = WebSocketConnection[F, WebSocketFrame, WebSocketFrame]
  def apply[F[_]](f: Request[F] => Resource[F, RawWebSocketConnection[F]])(
      implicit F: Sync[F]): WebSocketClient[F] = new DefaultWebSocketClient[F]() {
    override def run(r: Request[F]): Resource[F, RawWebSocketConnection[F]] = f(r)
  }

  abstract class DefaultWebSocketClient[F[_]](implicit F: Sync[F]) extends WebSocketClient[F] {
    override def connect(s: String): Resource[F, RawWebSocketConnection[F]] =
      run(Request(uri = Uri.unsafeFromString(s)))
    override def connect(u: Uri): Resource[F, RawWebSocketConnection[F]] = run(Request(uri = u))
    override def pipe(r: Request[F])(f: Pipe[F, WebSocketFrame, WebSocketFrame]): F[Unit] =
      run(r).use { ws =>
        ws.in.through(f).through(ws.out).compile.drain
      }
  }

  def mergeFrames(a: WebSocketFrame, b: WebSocketFrame): Option[WebSocketFrame] =
    (a, b) match {
      case (l: WebSocketFrame.Text, r: WebSocketFrame.Text) =>
        Some(WebSocketFrame.Text(l.str + r.str, r.last))
      case (l: WebSocketFrame.Binary, r: WebSocketFrame.Binary) =>
        Some(WebSocketFrame.Binary(l.data ++ r.data, r.last))
      case (l: WebSocketFrame.Text, r: WebSocketFrame.Continuation) =>
        Some(WebSocketFrame.Text(l.str + new String(r.data.toArray, UTF_8), r.last))
      case (l: WebSocketFrame.Binary, r: WebSocketFrame.Continuation) =>
        Some(WebSocketFrame.Binary(l.data ++ r.data, r.last))
      case _ => None
    }

  def mergeFrames(xs: Chain[WebSocketFrame]): Option[WebSocketFrame] =
    xs.iterator.drop(1).foldLeft(xs.headOption) { (a, f) =>
      a.flatMap { previous =>
        mergeFrames(previous, f)
      }
    }

  sealed trait TypedWebSocketPayload[T]
  case class Payload[T](payload: T) extends TypedWebSocketPayload[T]
  case class ControlFrame[T](underlying: WebSocketFrame.ControlFrame)
      extends TypedWebSocketPayload[T]

  object TypedWebSocketPayload {
    def apply[T](t: T): TypedWebSocketPayload[T] = Payload(t)
    def apply[T](underlying: WebSocketFrame.ControlFrame): TypedWebSocketPayload[T] =
      ControlFrame(underlying)
  }

  def decode[F[_], T](s: Stream[F, WebSocketFrame])(decoder: EntityDecoder[F, T])(
      implicit F: ApplicativeError[F, Throwable]): Stream[F, TypedWebSocketPayload[T]] =
    s.flatMap { frame =>
      frame match {
        case cf: WebSocketFrame.ControlFrame => Stream.emit(TypedWebSocketPayload[T](cf)).covary[F]
        case WebSocketFrame.Binary(data, true) =>
          Stream
            .eval(
              decoder
                .decode(Response[F]().withBodyStream(Stream.emits(data.toIterable.toList)), true)
                .value)
            .rethrow
            .map(TypedWebSocketPayload.apply[T])
        case WebSocketFrame.Text(data, true) =>
          Stream
            .eval(
              decoder
                .decode(
                  Response[F]().withBodyStream(
                    Stream.emits(data.getBytes(UTF_8).toIterable.toList)),
                  true)
                .value)
            .rethrow
            .map(TypedWebSocketPayload.apply[T])
        case _ => Stream.empty
      }
    }

  implicit class RawWebSocketConnectionOps[F[_]](wsc: RawWebSocketConnection[F]) {
    def as[O2, I2](
        implicit O2: EntityEncoder[F, O2],
        I2: EntityDecoder[F, I2],
        F: ApplicativeError[F, Throwable])
      : WebSocketConnection[F, TypedWebSocketPayload[O2], TypedWebSocketPayload[I2]] = {
      // merge together the continuation frames
      val newIn = wsc.in
        .zipWithScan1(Chain.empty[WebSocketFrame]) {
          case (fs, f) =>
            if (f.last) {
              Chain.empty[WebSocketFrame]
            } else {
              fs.append(f)
            }
        }
        .flatMap {
          case (f, fs) =>
            if (f.last) {
              mergeFrames(fs).map(Stream.emit).getOrElse(Stream.emits(fs.toList))
            } else {
              Stream.empty
            }
        }
      val decodedIn = decode[F, I2](newIn)(I2)

      val newOut = { s: Stream[F, O2] =>
          s.flatMap(O2.toEntity).through(s => fs2.text.utf8Decode[F])
        }

    }
  }

}
