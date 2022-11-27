// @GENERATOR:play-routes-compiler
// @SOURCE:/root/datahub/datahub-frontend/conf/routes
// @DATE:Fri Nov 18 06:26:01 EST 2022

package router

import play.core.routing._
import play.core.routing.HandlerInvokerFactory._

import play.api.mvc._

import _root_.controllers.Assets.Asset

class Routes(
  override val errorHandler: play.api.http.HttpErrorHandler, 
  // @LINE:7
  Application_1: controllers.Application,
  // @LINE:15
  AuthenticationController_0: controllers.AuthenticationController,
  // @LINE:19
  SsoCallbackController_5: controllers.SsoCallbackController,
  // @LINE:21
  CentralLogoutController_3: controllers.CentralLogoutController,
  // @LINE:36
  Assets_4: controllers.Assets,
  // @LINE:39
  TrackingController_2: controllers.TrackingController,
  val prefix: String
) extends GeneratedRouter {

   @javax.inject.Inject()
   def this(errorHandler: play.api.http.HttpErrorHandler,
    // @LINE:7
    Application_1: controllers.Application,
    // @LINE:15
    AuthenticationController_0: controllers.AuthenticationController,
    // @LINE:19
    SsoCallbackController_5: controllers.SsoCallbackController,
    // @LINE:21
    CentralLogoutController_3: controllers.CentralLogoutController,
    // @LINE:36
    Assets_4: controllers.Assets,
    // @LINE:39
    TrackingController_2: controllers.TrackingController
  ) = this(errorHandler, Application_1, AuthenticationController_0, SsoCallbackController_5, CentralLogoutController_3, Assets_4, TrackingController_2, "/")

  def withPrefix(addPrefix: String): Routes = {
    val prefix = play.api.routing.Router.concatPrefix(addPrefix, this.prefix)
    router.RoutesPrefix.setPrefix(prefix)
    new Routes(errorHandler, Application_1, AuthenticationController_0, SsoCallbackController_5, CentralLogoutController_3, Assets_4, TrackingController_2, prefix)
  }

  private[this] val defaultPrefix: String = {
    if (this.prefix.endsWith("/")) "" else "/"
  }

  def documentation = List(
    ("""GET""", this.prefix, """controllers.Application.index(path:String = "index.html")"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """admin""", """controllers.Application.healthcheck()"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """config""", """controllers.Application.appConfig()"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """authenticate""", """controllers.AuthenticationController.authenticate(request:Request)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """logIn""", """controllers.AuthenticationController.logIn(request:Request)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """signUp""", """controllers.AuthenticationController.signUp(request:Request)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """resetNativeUserCredentials""", """controllers.AuthenticationController.resetNativeUserCredentials(request:Request)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """callback/""" + "$" + """protocol<[^/]+>""", """controllers.SsoCallbackController.handleCallback(protocol:String)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """callback/""" + "$" + """protocol<[^/]+>""", """controllers.SsoCallbackController.handleCallback(protocol:String)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """logOut""", """controllers.CentralLogoutController.executeLogout()"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """api/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """api/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String)"""),
    ("""DELETE""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """api/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String)"""),
    ("""PUT""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """api/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """openapi/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """openapi/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String)"""),
    ("""DELETE""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """openapi/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String)"""),
    ("""PUT""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """openapi/""" + "$" + """path<.+>""", """controllers.Application.proxy(path:String)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """assets/""" + "$" + """file<.+>""", """controllers.Assets.at(path:String = "/public", file:String)"""),
    ("""POST""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """track""", """controllers.TrackingController.track(request:Request)"""),
    ("""GET""", this.prefix + (if(this.prefix.endsWith("/")) "" else "/") + """""" + "$" + """path<.+>""", """controllers.Application.index(path:String)"""),
    Nil
  ).foldLeft(List.empty[(String,String,String)]) { (s,e) => e.asInstanceOf[Any] match {
    case r @ (_,_,_) => s :+ r.asInstanceOf[(String,String,String)]
    case l => s ++ l.asInstanceOf[List[(String,String,String)]]
  }}


  // @LINE:7
  private[this] lazy val controllers_Application_index0_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix)))
  )
  private[this] lazy val controllers_Application_index0_invoker = createInvoker(
    Application_1.index(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "index",
      Seq(classOf[String]),
      "GET",
      this.prefix + """""",
      """ Home page
 serveAsset action requires a path string""",
      Seq()
    )
  )

  // @LINE:9
  private[this] lazy val controllers_Application_healthcheck1_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("admin")))
  )
  private[this] lazy val controllers_Application_healthcheck1_invoker = createInvoker(
    Application_1.healthcheck(),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "healthcheck",
      Nil,
      "GET",
      this.prefix + """admin""",
      """""",
      Seq()
    )
  )

  // @LINE:10
  private[this] lazy val controllers_Application_appConfig2_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("config")))
  )
  private[this] lazy val controllers_Application_appConfig2_invoker = createInvoker(
    Application_1.appConfig(),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "appConfig",
      Nil,
      "GET",
      this.prefix + """config""",
      """""",
      Seq()
    )
  )

  // @LINE:15
  private[this] lazy val controllers_AuthenticationController_authenticate3_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("authenticate")))
  )
  private[this] lazy val controllers_AuthenticationController_authenticate3_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      AuthenticationController_0.authenticate(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.AuthenticationController",
      "authenticate",
      Seq(classOf[play.mvc.Http.Request]),
      "GET",
      this.prefix + """authenticate""",
      """ Authentication in React""",
      Seq()
    )
  )

  // @LINE:16
  private[this] lazy val controllers_AuthenticationController_logIn4_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("logIn")))
  )
  private[this] lazy val controllers_AuthenticationController_logIn4_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      AuthenticationController_0.logIn(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.AuthenticationController",
      "logIn",
      Seq(classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """logIn""",
      """""",
      Seq()
    )
  )

  // @LINE:17
  private[this] lazy val controllers_AuthenticationController_signUp5_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("signUp")))
  )
  private[this] lazy val controllers_AuthenticationController_signUp5_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      AuthenticationController_0.signUp(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.AuthenticationController",
      "signUp",
      Seq(classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """signUp""",
      """""",
      Seq()
    )
  )

  // @LINE:18
  private[this] lazy val controllers_AuthenticationController_resetNativeUserCredentials6_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("resetNativeUserCredentials")))
  )
  private[this] lazy val controllers_AuthenticationController_resetNativeUserCredentials6_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      AuthenticationController_0.resetNativeUserCredentials(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.AuthenticationController",
      "resetNativeUserCredentials",
      Seq(classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """resetNativeUserCredentials""",
      """""",
      Seq()
    )
  )

  // @LINE:19
  private[this] lazy val controllers_SsoCallbackController_handleCallback7_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("callback/"), DynamicPart("protocol", """[^/]+""",true)))
  )
  private[this] lazy val controllers_SsoCallbackController_handleCallback7_invoker = createInvoker(
    SsoCallbackController_5.handleCallback(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.SsoCallbackController",
      "handleCallback",
      Seq(classOf[String]),
      "GET",
      this.prefix + """callback/""" + "$" + """protocol<[^/]+>""",
      """""",
      Seq()
    )
  )

  // @LINE:20
  private[this] lazy val controllers_SsoCallbackController_handleCallback8_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("callback/"), DynamicPart("protocol", """[^/]+""",true)))
  )
  private[this] lazy val controllers_SsoCallbackController_handleCallback8_invoker = createInvoker(
    SsoCallbackController_5.handleCallback(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.SsoCallbackController",
      "handleCallback",
      Seq(classOf[String]),
      "POST",
      this.prefix + """callback/""" + "$" + """protocol<[^/]+>""",
      """""",
      Seq()
    )
  )

  // @LINE:21
  private[this] lazy val controllers_CentralLogoutController_executeLogout9_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("logOut")))
  )
  private[this] lazy val controllers_CentralLogoutController_executeLogout9_invoker = createInvoker(
    CentralLogoutController_3.executeLogout(),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.CentralLogoutController",
      "executeLogout",
      Nil,
      "GET",
      this.prefix + """logOut""",
      """""",
      Seq()
    )
  )

  // @LINE:24
  private[this] lazy val controllers_Application_proxy10_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("api/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy10_invoker = createInvoker(
    Application_1.proxy(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String]),
      "GET",
      this.prefix + """api/""" + "$" + """path<.+>""",
      """ Proxies API requests to the metadata service api""",
      Seq()
    )
  )

  // @LINE:25
  private[this] lazy val controllers_Application_proxy11_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("api/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy11_invoker = createInvoker(
    Application_1.proxy(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String]),
      "POST",
      this.prefix + """api/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:26
  private[this] lazy val controllers_Application_proxy12_route = Route("DELETE",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("api/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy12_invoker = createInvoker(
    Application_1.proxy(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String]),
      "DELETE",
      this.prefix + """api/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:27
  private[this] lazy val controllers_Application_proxy13_route = Route("PUT",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("api/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy13_invoker = createInvoker(
    Application_1.proxy(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String]),
      "PUT",
      this.prefix + """api/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:30
  private[this] lazy val controllers_Application_proxy14_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("openapi/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy14_invoker = createInvoker(
    Application_1.proxy(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String]),
      "GET",
      this.prefix + """openapi/""" + "$" + """path<.+>""",
      """ Proxies API requests to the metadata service api""",
      Seq()
    )
  )

  // @LINE:31
  private[this] lazy val controllers_Application_proxy15_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("openapi/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy15_invoker = createInvoker(
    Application_1.proxy(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String]),
      "POST",
      this.prefix + """openapi/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:32
  private[this] lazy val controllers_Application_proxy16_route = Route("DELETE",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("openapi/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy16_invoker = createInvoker(
    Application_1.proxy(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String]),
      "DELETE",
      this.prefix + """openapi/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:33
  private[this] lazy val controllers_Application_proxy17_route = Route("PUT",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("openapi/"), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_proxy17_invoker = createInvoker(
    Application_1.proxy(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "proxy",
      Seq(classOf[String]),
      "PUT",
      this.prefix + """openapi/""" + "$" + """path<.+>""",
      """""",
      Seq()
    )
  )

  // @LINE:36
  private[this] lazy val controllers_Assets_at18_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("assets/"), DynamicPart("file", """.+""",false)))
  )
  private[this] lazy val controllers_Assets_at18_invoker = createInvoker(
    Assets_4.at(fakeValue[String], fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Assets",
      "at",
      Seq(classOf[String], classOf[String]),
      "GET",
      this.prefix + """assets/""" + "$" + """file<.+>""",
      """ Map static resources from the /public folder to the /assets URL path""",
      Seq()
    )
  )

  // @LINE:39
  private[this] lazy val controllers_TrackingController_track19_route = Route("POST",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), StaticPart("track")))
  )
  private[this] lazy val controllers_TrackingController_track19_invoker = createInvoker(
    
    (req:play.mvc.Http.Request) =>
      TrackingController_2.track(fakeValue[play.mvc.Http.Request]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.TrackingController",
      "track",
      Seq(classOf[play.mvc.Http.Request]),
      "POST",
      this.prefix + """track""",
      """ Analytics route""",
      Seq()
    )
  )

  // @LINE:42
  private[this] lazy val controllers_Application_index20_route = Route("GET",
    PathPattern(List(StaticPart(this.prefix), StaticPart(this.defaultPrefix), DynamicPart("path", """.+""",false)))
  )
  private[this] lazy val controllers_Application_index20_invoker = createInvoker(
    Application_1.index(fakeValue[String]),
    play.api.routing.HandlerDef(this.getClass.getClassLoader,
      "router",
      "controllers.Application",
      "index",
      Seq(classOf[String]),
      "GET",
      this.prefix + """""" + "$" + """path<.+>""",
      """ Wildcard route accepts any routes and delegates to serveAsset which in turn serves the React Bundle""",
      Seq()
    )
  )


  def routes: PartialFunction[RequestHeader, Handler] = {
  
    // @LINE:7
    case controllers_Application_index0_route(params@_) =>
      call(Param[String]("path", Right("index.html"))) { (path) =>
        controllers_Application_index0_invoker.call(Application_1.index(path))
      }
  
    // @LINE:9
    case controllers_Application_healthcheck1_route(params@_) =>
      call { 
        controllers_Application_healthcheck1_invoker.call(Application_1.healthcheck())
      }
  
    // @LINE:10
    case controllers_Application_appConfig2_route(params@_) =>
      call { 
        controllers_Application_appConfig2_invoker.call(Application_1.appConfig())
      }
  
    // @LINE:15
    case controllers_AuthenticationController_authenticate3_route(params@_) =>
      call { 
        controllers_AuthenticationController_authenticate3_invoker.call(
          req => AuthenticationController_0.authenticate(req))
      }
  
    // @LINE:16
    case controllers_AuthenticationController_logIn4_route(params@_) =>
      call { 
        controllers_AuthenticationController_logIn4_invoker.call(
          req => AuthenticationController_0.logIn(req))
      }
  
    // @LINE:17
    case controllers_AuthenticationController_signUp5_route(params@_) =>
      call { 
        controllers_AuthenticationController_signUp5_invoker.call(
          req => AuthenticationController_0.signUp(req))
      }
  
    // @LINE:18
    case controllers_AuthenticationController_resetNativeUserCredentials6_route(params@_) =>
      call { 
        controllers_AuthenticationController_resetNativeUserCredentials6_invoker.call(
          req => AuthenticationController_0.resetNativeUserCredentials(req))
      }
  
    // @LINE:19
    case controllers_SsoCallbackController_handleCallback7_route(params@_) =>
      call(params.fromPath[String]("protocol", None)) { (protocol) =>
        controllers_SsoCallbackController_handleCallback7_invoker.call(SsoCallbackController_5.handleCallback(protocol))
      }
  
    // @LINE:20
    case controllers_SsoCallbackController_handleCallback8_route(params@_) =>
      call(params.fromPath[String]("protocol", None)) { (protocol) =>
        controllers_SsoCallbackController_handleCallback8_invoker.call(SsoCallbackController_5.handleCallback(protocol))
      }
  
    // @LINE:21
    case controllers_CentralLogoutController_executeLogout9_route(params@_) =>
      call { 
        controllers_CentralLogoutController_executeLogout9_invoker.call(CentralLogoutController_3.executeLogout())
      }
  
    // @LINE:24
    case controllers_Application_proxy10_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy10_invoker.call(Application_1.proxy(path))
      }
  
    // @LINE:25
    case controllers_Application_proxy11_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy11_invoker.call(Application_1.proxy(path))
      }
  
    // @LINE:26
    case controllers_Application_proxy12_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy12_invoker.call(Application_1.proxy(path))
      }
  
    // @LINE:27
    case controllers_Application_proxy13_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy13_invoker.call(Application_1.proxy(path))
      }
  
    // @LINE:30
    case controllers_Application_proxy14_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy14_invoker.call(Application_1.proxy(path))
      }
  
    // @LINE:31
    case controllers_Application_proxy15_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy15_invoker.call(Application_1.proxy(path))
      }
  
    // @LINE:32
    case controllers_Application_proxy16_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy16_invoker.call(Application_1.proxy(path))
      }
  
    // @LINE:33
    case controllers_Application_proxy17_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_proxy17_invoker.call(Application_1.proxy(path))
      }
  
    // @LINE:36
    case controllers_Assets_at18_route(params@_) =>
      call(Param[String]("path", Right("/public")), params.fromPath[String]("file", None)) { (path, file) =>
        controllers_Assets_at18_invoker.call(Assets_4.at(path, file))
      }
  
    // @LINE:39
    case controllers_TrackingController_track19_route(params@_) =>
      call { 
        controllers_TrackingController_track19_invoker.call(
          req => TrackingController_2.track(req))
      }
  
    // @LINE:42
    case controllers_Application_index20_route(params@_) =>
      call(params.fromPath[String]("path", None)) { (path) =>
        controllers_Application_index20_invoker.call(Application_1.index(path))
      }
  }
}
