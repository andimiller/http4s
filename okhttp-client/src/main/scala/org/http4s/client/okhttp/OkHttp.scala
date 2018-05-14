package org.http4s.client.okhttp

import java.io.IOException

import cats._, cats.syntax._
import cats.data._
import cats.effect._
import cats.effect.implicits._
import okhttp3.{Call, Callback, OkHttpClient, Protocol, RequestBody, WebSocket, WebSocketListener, Headers => OKHeaders, MediaType => OKMediaType, Request => OKRequest, Response => OKResponse}
import okio.{BufferedSink, ByteString}
import org.http4s.{Header, Headers, HttpVersion, Method, Request, Response, Status}
import org.http4s.client.{Client, DisposableResponse, DisposableWebsocket, WebsocketClient}
import fs2.Stream._
import fs2._
import fs2.io._
import org.http4s.websocket.{Websocket, WebsocketBits}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

object OkHttp {

  def defaultConfig[F[_]]()(implicit F: Effect[F]): F[OkHttpClient.Builder] = F.delay {
    new OkHttpClient.Builder()
      .protocols(
        List(
          Protocol.HTTP_2,
          Protocol.HTTP_1_1
        ).asJava)
  }

  def apply[F[_]](config: F[OkHttpClient.Builder] = null)(
      implicit F: Effect[F],
      ec: ExecutionContext): F[Client[F]] = F.map(Option(config).getOrElse(defaultConfig[F]())) {
    c =>
      val client = c.build()
      Client(
        Kleisli { req =>
          F.async[DisposableResponse[F]] { cb =>
            client.newCall(toOkHttpRequest(req)).enqueue(handler(cb))
            ()
          }
        },
        F.delay({
          client.dispatcher.executorService().shutdown()
          client.connectionPool().evictAll()
          if (client.cache() != null) {
            client.cache().close()
          }
        })
      )
  }

  def stream[F[_]](config: F[OkHttpClient.Builder] = defaultConfig[F]())(
      implicit F: Effect[F],
      ec: ExecutionContext): Stream[F, Client[F]] =
    Stream.bracket(apply(config))(c => Stream.emit(c), _.shutdown)

  private def handler[F[_]](cb: Either[Throwable, DisposableResponse[F]] => Unit)(
      implicit F: Effect[F],
      ec: ExecutionContext): Callback =
    new Callback {
      override def onFailure(call: Call, e: IOException): Unit =
        ec.execute(new Runnable { override def run(): Unit = cb(Left(e)) })

      override def onResponse(call: Call, response: OKResponse): Unit = {
        val protocol = response.protocol() match {
          case Protocol.HTTP_2 => HttpVersion.`HTTP/2.0`
          case Protocol.HTTP_1_1 => HttpVersion.`HTTP/1.1`
          case Protocol.HTTP_1_0 => HttpVersion.`HTTP/1.0`
          case _ => HttpVersion.`HTTP/1.1`
        }
        val bodyStream = response.body.byteStream()
        val dr = new DisposableResponse[F](
          Response[F](headers = getHeaders(response), httpVersion = protocol)
            .withStatus(
              Status
                .fromInt(response.code())
                .getOrElse(Status.apply(response.code())))
            .withBodyStream(
              readInputStream(F.pure(bodyStream), chunkSize = 1024, closeAfterUse = true)),
          F.delay({ bodyStream.close(); () })
        )
        ec.execute(new Runnable {
          override def run(): Unit = cb(Right(dr))
        })
      }
    }

  private def getHeaders(response: OKResponse): Headers =
    Headers(response.headers().names().asScala.toList.flatMap { v =>
      response.headers().values(v).asScala.map(Header(v, _))
    })

  private def toOkHttpRequest[F[_]](req: Request[F])(implicit F: Effect[F]): OKRequest = {
    val body = req match {
      case _ if req.isChunked || req.contentLength.isDefined =>
        new RequestBody {
          override def contentType(): OKMediaType =
            req.contentType.map(c => OKMediaType.parse(c.toString())).orNull

          override def writeTo(sink: BufferedSink): Unit =
            req.body.chunks
              .map(_.toArray)
              .to(Sink { b: Array[Byte] =>
                F.delay {
                  sink.write(b); ()
                }
              })
              .compile
              .drain
              .runAsync(_ => IO.unit)
              .unsafeRunSync()
        }
      // if it's a GET or HEAD, okhttp wants us to pass null
      case _ if req.method == Method.GET || req.method == Method.HEAD => null
      // for anything else we can pass a body which produces no output
      case _ =>
        new RequestBody {
          override def contentType(): OKMediaType = null
          override def writeTo(sink: BufferedSink): Unit = ()
        }
    }

    new OKRequest.Builder()
      .headers(OKHeaders.of(req.headers.toList.map(h => (h.name.value, h.value)).toMap.asJava))
      .method(req.method.toString(), body)
      .url(req.uri.toString())
      .build()
  }

  // websockets

  def websocket[F[_]](config: F[OkHttpClient.Builder] = null)(
      implicit F: Effect[F]): WebsocketClient[F] =
    F.map(Option(config).getOrElse(defaultConfig[F]())) { c =>
      val client = c.build()

      WebsocketClient(
        Kleisli { req =>
          F.async[DisposableWebsocket[F]] { cb =>
            LiftIO[F].
            val output = fs2.async.mutable.Queue.bounded[F, Array[Byte]](100).unsafeRunSync()
            val outputBits = output.dequeue.map(WebsocketBits.Binary(_)).covaryOutput[WebsocketBits.WebSocketFrame]
            val listener = new WebSocketListener {
              override def onMessage(webSocket: WebSocket, bytes: ByteString): Unit = {
                output.enqueue1(bytes.toByteArray)
                ()
              }
            }
            val ws = client.newWebSocket(toOkHttpRequest(req), listener)
            val input: Pipe[F, WebsocketBits.WebSocketFrame, Unit] = Sink {b: WebsocketBits.WebSocketFrame => F.delay { ws.send(ByteString.of(b.data:_*)); () } }
            cb(Right(DisposableWebsocket(Websocket[F](outputBits, input), F.delay { ws.cancel() })))
            ()
          }
        },
        F.delay {
          client.dispatcher.executorService().shutdown()
          client.connectionPool().evictAll()
          if (client.cache() != null) {
            client.cache().close()
          }
        }
      )

      Client(
        Kleisli { req =>
          F.async[DisposableResponse[F]] { cb =>
            client.newCall(toOkHttpRequest(req)).enqueue(handler(cb))
            ()
          }
        },
        F.delay({
          client.dispatcher.executorService().shutdown()
          client.connectionPool().evictAll()
          if (client.cache() != null) {
            client.cache().close()
          }
        })
      )
    }
}
