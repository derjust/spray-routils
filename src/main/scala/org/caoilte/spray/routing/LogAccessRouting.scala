package org.caoilte.spray.routing

import akka.io.Tcp
import com.typesafe.config.Config
import spray.can.Http
import spray.can.Http.{Command, RegisterChunkHandler}
import spray.httpx.marshalling.{ToResponseMarshallingContext, ToResponseMarshaller}
import spray.routing._
import akka.actor._
import scala.concurrent.duration._
import spray.util.{SettingsCompanion, LoggingContext}
import spray.http._
import RequestAccessLogger._
import scala.concurrent.duration._
import spray.util._


trait AccessLogger {
  def logAccess(request:HttpRequest, response:HttpResponse, time:Long):Unit
  def accessAlreadyLogged(request:HttpRequest, response:HttpResponse, time:Long):Unit
}

object RequestAccessLogger {

  def defaultTimeStampCalculator():(Unit => Long) = {
    val startTime = System.currentTimeMillis
    Unit => System.currentTimeMillis() - startTime
  }

  def props(ctx: RequestContext, accessLogger: AccessLogger,
            timeStampCalculator:(Unit => Long), requestTimeout:FiniteDuration):Props = {
    Props(new RequestAccessLogger(ctx, accessLogger, timeStampCalculator, requestTimeout))
  }

  case object RequestLoggingTimeout

}

class RequestAccessLogger(ctx: RequestContext, accessLogger: AccessLogger,
                          timeStampCalculator:(Unit => Long), requestTimeout:FiniteDuration) extends Actor with ActorLogging {
  import ctx._

  import context.dispatcher
  var cancellable:Cancellable = context.system.scheduler.scheduleOnce(requestTimeout, self, RequestLoggingTimeout)
  var chunkedResponse:Option[HttpResponse] = Option.empty

  def handleChunkedResponseStart(wrapper: Any, response: HttpResponse) = {
    // In case a chunk transfer is started, store the response for later logging
    chunkedResponse = Option(response)
    responder forward wrapper
  }

  def receive = {
    case RequestLoggingTimeout => {
      val errorResponse = HttpResponse(
        StatusCodes.InternalServerError,
        HttpEntity(s"The RequestAccessLogger timed out waiting for the request to complete after " +
          s"'${requestTimeout}'."))

      accessLogger.logAccess(request, errorResponse, requestTimeout.toMillis)

      if (chunkedResponse.isDefined) {
        // There was already a ChunkedResponseStart thus the transfer is in-flight. There is no reasonable response
        // that could be sent because that would be queued by spray itself - and we can not predict what the client will
        // do with any content we append at the current state of the chunked message stream.
        // Therefore the whole connection is just aborted
        log.debug(s"timed out waiting for the request to complete after ${requestTimeout} - Aborting connection")
        ctx.responder ! Tcp.Abort
      } else {
        log.debug(s"timed out waiting for the request to complete after ${requestTimeout} - Sending ${errorResponse}")
        ctx.complete(errorResponse)
      }
      context.stop(self)
    }
    case response:HttpResponse => {
      cancellable.cancel()
      accessLogger.logAccess(request, response, timeStampCalculator(Unit))
      forwardMsgAndStop(response)
    }

    case confirmed@Confirmed(ChunkedResponseStart(response), _) => handleChunkedResponseStart(confirmed, response)
    case unconfirmed@ChunkedResponseStart(response) => handleChunkedResponseStart(unconfirmed, response)

    case confirmed@(Confirmed(MessageChunk(_,_), _) | MessageChunk) => {
      // Each chunk is considered a heart-beat: We are still producing output and the client is still consuming
      // - therefore reset the requestTimeout schedule
      cancellable.cancel()
      cancellable = context.system.scheduler.scheduleOnce(requestTimeout, self, RequestLoggingTimeout)

      responder forward confirmed
    }

    case confirmed@(Confirmed(ChunkedMessageEnd, _) | ChunkedMessageEnd) => {
      // Handled like HttpResponse: provide the (previously saved) ChunkedResponseStart for logging and quit
      cancellable.cancel()
      accessLogger.logAccess(request, chunkedResponse.get, timeStampCalculator(Unit))
      forwardMsgAndStop(confirmed)
    }

    case other => {
      responder forward other
    }
  }

  def forwardMsgAndStop(msg:Any) {
    responder forward msg
    context.stop(self)
  }
}

object SingleAccessLogger {
  case class AccessLogRequest(request:HttpRequest, response:HttpResponse, time:Long)
  case object LogState
}

trait LogAccessRouting extends HttpService {
  val requestTimeout:FiniteDuration
  val accessLogger:AccessLogger

  override def runRoute(route: Route)(implicit eh: ExceptionHandler, rh: RejectionHandler, ac: ActorContext,
                                      rs: RoutingSettings, log: LoggingContext): Actor.Receive = {

    def attachLoggingInterceptorToRequest(request: HttpRequest,
                                 timeStampCalculator: (Unit => Long) = defaultTimeStampCalculator()):RequestContext = {

      attachLoggingInterceptorToCtx(
        RequestContext(request, ac.sender(), request.uri.path).withDefaultSender(ac.self), timeStampCalculator
      )
    }
    def attachLoggingInterceptorToCtx(ctx:RequestContext,
                                 timeStampCalculator: (Unit => Long) = defaultTimeStampCalculator()):RequestContext = {

      val loggingInterceptor = ac.actorOf(
        RequestAccessLogger.props(ctx, accessLogger, timeStampCalculator, requestTimeout)
      )
      ctx.withResponder(loggingInterceptor)
    }

    {
      case request: HttpRequest ⇒ {
        val ctx = attachLoggingInterceptorToRequest(request)
        super.runRoute(route)(eh, rh, ac, rs, log)(ctx)
      }
      case ctx: RequestContext ⇒ {
        super.runRoute(route)(eh, rh, ac, rs, log)(attachLoggingInterceptorToCtx(ctx))
      }
      case other => super.runRoute(route)(eh, rh, ac, rs, log)(other)
    }
  }
}

case class LogAccessRoutingSettings(requestTimeout: FiniteDuration)

object LogAccessRoutingSettings extends SettingsCompanion[LogAccessRoutingSettings]("spray.can.server") {
  override def fromSubConfig(c: Config): LogAccessRoutingSettings = {

    val sprayDuration:Duration = c getDuration "request-timeout"
    require(!sprayDuration.isFinite(), "LogAccessRouting requires spray.can.server.request-timeout to be set as 'infinite'")

    val duration:Duration = c getDuration "routils-request-timeout"
    require(duration.isFinite(), "LogAccessRouting requires spray.can.server.routils-request-timeout to be set with a finite duration")
    apply(duration.asInstanceOf[FiniteDuration])
  }
}

trait LogAccessRoutingActor extends HttpServiceActor with LogAccessRouting {

  val accessLogger:AccessLogger
  val requestTimeout:FiniteDuration = LogAccessRoutingSettings(context.system).requestTimeout

  var singleAccessLoggerRef:ActorRef = _
}