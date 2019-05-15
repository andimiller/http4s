package org.http4s
package client
package asynchttpclient

import cats.effect._
import cats.implicits.{catsSyntaxEither => _, _}
import cats.effect.implicits._
import fs2.Stream._
import fs2._
import fs2.interop.reactivestreams.{StreamSubscriber, StreamUnicastPublisher}
import _root_.io.netty.handler.codec.http.{DefaultHttpHeaders, HttpHeaders}
import _root_.io.netty.buffer.Unpooled
import _root_.io.netty.util.concurrent.Future
import org.asynchttpclient.AsyncHandler.State
import org.asynchttpclient.handler.StreamedAsyncHandler
import org.asynchttpclient.request.body.generator.{BodyGenerator, ReactiveStreamsBodyGenerator}
import org.asynchttpclient.{Request => AsyncRequest, Response => _, _}
import org.http4s.internal.invokeCallback
import org.http4s.util.threads._
import org.log4s.getLogger
import org.reactivestreams.Publisher

import scala.collection.JavaConverters._
import _root_.io.netty.handler.codec.http.cookie.Cookie
import org.asynchttpclient.uri.Uri
import org.asynchttpclient.cookie.CookieStore
import org.asynchttpclient.ws.{WebSocketListener, WebSocketUpgradeHandler}
import org.http4s.client.WebSocketClient.RawWebSocketConnection
import org.http4s.websocket.WebSocketFrame
import scodec.bits.ByteVector

object AsyncHttpClient {
  private[this] val logger = getLogger

  val defaultConfig = new DefaultAsyncHttpClientConfig.Builder()
    .setMaxConnectionsPerHost(200)
    .setMaxConnections(400)
    .setRequestTimeout(60000)
    .setThreadFactory(threadFactory(name = { i =>
      s"http4s-async-http-client-worker-${i}"
    }))
    .setCookieStore(new NoOpCookieStore)
    .build()

  /**
    * Allocates a Client and its shutdown mechanism for freeing resources.
    */
  def allocate[F[_]](config: AsyncHttpClientConfig = defaultConfig)(
      implicit F: ConcurrentEffect[F]): F[(Client[F], F[Unit])] =
    F.delay(new DefaultAsyncHttpClient(config))
      .map(c =>
        (Client[F] { req =>
          Resource(F.async[(Response[F], F[Unit])] { cb =>
            c.executeRequest(toAsyncRequest(req), asyncHandler(cb))
            ()
          })
        }, F.delay(c.close)))

  /**
    * Allocates a WebSocketClient and its shutdown mechanism for freeing resources.
    */
  def allocateWebSocket[F[_]](config: AsyncHttpClientConfig = defaultConfig)(
      implicit F: ConcurrentEffect[F]): F[(WebSocketClient[F], F[Unit])] =
    F.delay(new DefaultAsyncHttpClient(config))
      .map(c =>
        (WebSocketClient[F] { req =>
          Resource(F.async[(RawWebSocketConnection[F], F[Unit])] { cb =>
            c.executeRequest(toWebSocketAsyncRequest(req), webSocketHandler(cb))
            ()
          })
        }, F.delay(c.close)))

  /**
    * Create an HTTP client based on the AsyncHttpClient library
    *
    * @param config configuration for the client
    * @param ec The ExecutionContext to run responses on
    */
  def resource[F[_]](config: AsyncHttpClientConfig = defaultConfig)(
      implicit F: ConcurrentEffect[F]): Resource[F, Client[F]] =
    Resource(allocate(config))

  /**
    * Create a WebSocket client based on the AsyncHttpClient library
    *
    * @param config configuration for the client
    */
  def resourceWebSocket[F[_]](config: AsyncHttpClientConfig = defaultConfig)(
      implicit F: ConcurrentEffect[F]): Resource[F, WebSocketClient[F]] =
    Resource(allocateWebSocket(config))

  /**
    * Create a bracketed HTTP client based on the AsyncHttpClient library.
    *
    * @param config configuration for the client
    * @param ec The ExecutionContext to run responses on
    * @return a singleton stream of the client.  The client will be
    * shutdown when the stream terminates.
    */
  def stream[F[_]](config: AsyncHttpClientConfig = defaultConfig)(
      implicit F: ConcurrentEffect[F]): Stream[F, Client[F]] =
    Stream.resource(resource(config))

  private def asyncHandler[F[_]](cb: Callback[(Response[F], F[Unit])])(
      implicit F: ConcurrentEffect[F]) =
    new StreamedAsyncHandler[Unit] {
      var state: State = State.CONTINUE
      var response: Response[F] = Response()
      val dispose = F.delay { state = State.ABORT }

      override def onStream(publisher: Publisher[HttpResponseBodyPart]): State = {
        // backpressure is handled by requests to the reactive streams subscription
        StreamSubscriber[F, HttpResponseBodyPart]
          .map { subscriber =>
            val body = subscriber.stream.flatMap(part => chunk(Chunk.bytes(part.getBodyPartBytes)))
            response = response.copy(body = body)
            // Run this before we return the response, lest we violate
            // Rule 3.16 of the reactive streams spec.
            publisher.subscribe(subscriber)
            // We have a fully formed response now.  Complete the
            // callback, rather than waiting for onComplete, or else we'll
            // buffer the entire response before we return it for
            // streaming consumption.
            invokeCallback(logger)(cb(Right(response -> dispose)))
          }
          .runAsync(_ => IO.unit)
          .unsafeRunSync()
        state
      }

      override def onBodyPartReceived(httpResponseBodyPart: HttpResponseBodyPart): State =
        throw org.http4s.util.bug("Expected it to call onStream instead.")

      override def onStatusReceived(status: HttpResponseStatus): State = {
        response = response.copy(status = getStatus(status))
        state
      }

      override def onHeadersReceived(headers: HttpHeaders): State = {
        response = response.copy(headers = getHeaders(headers))
        state
      }

      override def onThrowable(throwable: Throwable): Unit =
        invokeCallback(logger)(cb(Left(throwable)))

      override def onCompleted(): Unit = {
        // Don't close here.  onStream may still be being called.
      }
    }

  private def nettyFutureToF[F[_]: ConcurrentEffect, T](f: Future[T]): F[T] =
    ConcurrentEffect[F].async[T] { cb =>
      val _ = f.addListener(
        (future: Future[T]) =>
          if (future.isSuccess) {
            cb(Right(future.get()))
          } else {
            cb(Left(future.cause()))
        }
      )
    }

  private def webSocketHandler[F[_]](cb: Callback[(RawWebSocketConnection[F], F[Unit])])(
      implicit F: ConcurrentEffect[F]) =
    new WebSocketUpgradeHandler.Builder()
      .addWebSocketListener(new WebSocketListener {
        var opened: Boolean = false
        val sent = fs2.concurrent.Queue.unbounded[F, WebSocketFrame].toIO.unsafeRunSync()

        override def onOpen(websocket: ws.WebSocket): Unit = {
          opened = true
          val receive: Pipe[F, WebSocketFrame, Unit] = {
            wsfs: Stream[F, WebSocketFrame] =>
              wsfs.evalMap {
                case WebSocketFrame.Text(str, last) =>
                  nettyFutureToF[F, Void](websocket.sendTextFrame(str, last, 0)).void
                case WebSocketFrame.Binary(bv, last) =>
                  nettyFutureToF[F, Void](websocket.sendBinaryFrame(bv.toArray, last, 0)).void
                case WebSocketFrame.Ping(data) =>
                  nettyFutureToF[F, Void](websocket.sendPingFrame(data.toArray)).void
                case WebSocketFrame.Pong(data) =>
                  nettyFutureToF[F, Void](websocket.sendPongFrame(data.toArray)).void
                case WebSocketFrame.Continuation(data, last) =>
                  nettyFutureToF[F, Void](websocket.sendContinuationFrame(data.toArray, last, 0)).void
                case close: WebSocketFrame.Close =>
                  nettyFutureToF[F, Void](websocket.sendCloseFrame(close.closeCode, "")).void
              }
          }
          val send: Stream[F, WebSocketFrame] = sent.dequeue
          cb(
            Right(
              (
                WebSocketConnection(send, receive, F.unit),
                F.unit.flatMap { _ =>
                  nettyFutureToF[F, Void](websocket.sendCloseFrame()).void
                }
              ))
          )
        }
        override def onClose(websocket: ws.WebSocket, code: Int, reason: String): Unit =
          sent
            .enqueue1(WebSocketFrame.Close(code, reason).leftMap(_ => WebSocketFrame.Close()).merge)
            .runAsync(_ => IO.unit)
            .unsafeRunSync()

        override def onBinaryFrame(payload: Array[Byte], finalFragment: Boolean, rsv: Int): Unit =
          sent
            .enqueue1(WebSocketFrame.Binary(ByteVector(payload), finalFragment))
            .runAsync(_ => IO.unit)
            .unsafeRunSync()

        override def onTextFrame(payload: String, finalFragment: Boolean, rsv: Int): Unit =
          sent
            .enqueue1(WebSocketFrame.Text(payload, finalFragment))
            .runAsync(_ => IO.unit)
            .unsafeRunSync()

        override def onPingFrame(payload: Array[Byte]): Unit =
          sent
            .enqueue1(WebSocketFrame.Ping(ByteVector(payload)))
            .runAsync(_ => IO.unit)
            .unsafeRunSync()

        override def onPongFrame(payload: Array[Byte]): Unit =
          sent
            .enqueue1(WebSocketFrame.Pong(ByteVector(payload)))
            .runAsync(_ => IO.unit)
            .unsafeRunSync()

        override def onError(t: Throwable): Unit = {
          logger.error(t)("error from async-http-client's websocket handler")
          if (!opened) {
            cb(Left(t))
          }
        }
      })
      .build()

  private def toAsyncRequest[F[_]: ConcurrentEffect](request: Request[F]): AsyncRequest = {
    val headers = new DefaultHttpHeaders
    for (h <- request.headers.toList)
      headers.add(h.name.value, h.value)
    new RequestBuilder(request.method.renderString)
      .setUrl(request.uri.renderString)
      .setHeaders(headers)
      .setBody(getBodyGenerator(request))
      .build()
  }

  private def toWebSocketAsyncRequest[F[_]: ConcurrentEffect](request: Request[F]): AsyncRequest = {
    val headers = new DefaultHttpHeaders
    for (h <- request.headers.toList)
      headers.add(h.name.value, h.value)
    new RequestBuilder(request.method.renderString)
      .setUrl(request.uri.renderString)
      .setHeaders(headers)
      .build()
  }

  private def getBodyGenerator[F[_]: ConcurrentEffect](req: Request[F]): BodyGenerator = {
    val publisher = StreamUnicastPublisher(
      req.body.chunks.map(chunk => Unpooled.wrappedBuffer(chunk.toArray)))
    if (req.isChunked) new ReactiveStreamsBodyGenerator(publisher, -1)
    else
      req.contentLength match {
        case Some(len) => new ReactiveStreamsBodyGenerator(publisher, len)
        case None => EmptyBodyGenerator
      }
  }

  private def getStatus(status: HttpResponseStatus): Status =
    Status.fromInt(status.getStatusCode).valueOr(throw _)

  private def getHeaders(headers: HttpHeaders): Headers =
    Headers(headers.asScala.map { header =>
      Header(header.getKey, header.getValue)
    }.toList)
  private class NoOpCookieStore extends CookieStore {
    val empty: java.util.List[Cookie] = new java.util.ArrayList()
    override def add(uri: Uri, cookie: Cookie): Unit = ()
    override def get(uri: Uri): java.util.List[Cookie] = empty
    override def getAll(): java.util.List[Cookie] = empty
    override def remove(pred: java.util.function.Predicate[Cookie]): Boolean = false
    override def clear(): Boolean = false
  }
}
