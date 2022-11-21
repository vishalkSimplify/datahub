// @GENERATOR:play-routes-compiler
// @SOURCE:/root/datahub/datahub-frontend/conf/routes
// @DATE:Mon Oct 10 17:22:23 IST 2022

import play.api.routing.JavaScriptReverseRoute


import _root_.controllers.Assets.Asset

// @LINE:7
package controllers.javascript {

  // @LINE:39
  class ReverseTrackingController(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:39
    def track: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.TrackingController.track",
      """
        function() {
          return _wA({method:"POST", url:"""" + _prefix + { _defaultPrefix } + """" + "track"})
        }
      """
    )
  
  }

  // @LINE:36
  class ReverseAssets(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:36
    def at: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Assets.at",
      """
        function(file1) {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "assets/" + (""" + implicitly[play.api.mvc.PathBindable[String]].javascriptUnbind + """)("file", file1)})
        }
      """
    )
  
  }

  // @LINE:15
  class ReverseAuthenticationController(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:16
    def logIn: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.AuthenticationController.logIn",
      """
        function() {
          return _wA({method:"POST", url:"""" + _prefix + { _defaultPrefix } + """" + "logIn"})
        }
      """
    )
  
    // @LINE:17
    def signUp: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.AuthenticationController.signUp",
      """
        function() {
          return _wA({method:"POST", url:"""" + _prefix + { _defaultPrefix } + """" + "signUp"})
        }
      """
    )
  
    // @LINE:15
    def authenticate: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.AuthenticationController.authenticate",
      """
        function() {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "authenticate"})
        }
      """
    )
  
    // @LINE:18
    def resetNativeUserCredentials: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.AuthenticationController.resetNativeUserCredentials",
      """
        function() {
          return _wA({method:"POST", url:"""" + _prefix + { _defaultPrefix } + """" + "resetNativeUserCredentials"})
        }
      """
    )
  
  }

  // @LINE:19
  class ReverseSsoCallbackController(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:19
    def handleCallback: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.SsoCallbackController.handleCallback",
      """
        function(protocol0) {
        
          if (true) {
            return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "callback/" + encodeURIComponent((""" + implicitly[play.api.mvc.PathBindable[String]].javascriptUnbind + """)("protocol", protocol0))})
          }
        
        }
      """
    )
  
  }

  // @LINE:21
  class ReverseCentralLogoutController(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:21
    def executeLogout: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.CentralLogoutController.executeLogout",
      """
        function() {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "logOut"})
        }
      """
    )
  
  }

  // @LINE:7
  class ReverseApplication(_prefix: => String) {

    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:24
    def proxy: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Application.proxy",
      """
        function(path0) {
        
          if (true) {
            return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "api/" + (""" + implicitly[play.api.mvc.PathBindable[String]].javascriptUnbind + """)("path", path0)})
          }
        
        }
      """
    )
  
    // @LINE:9
    def healthcheck: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Application.healthcheck",
      """
        function() {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "admin"})
        }
      """
    )
  
    // @LINE:7
    def index: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Application.index",
      """
        function(path0) {
        
          if (path0 == """ + implicitly[play.api.mvc.JavascriptLiteral[String]].to("index.html") + """) {
            return _wA({method:"GET", url:"""" + _prefix + """"})
          }
        
          if (true) {
            return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + (""" + implicitly[play.api.mvc.PathBindable[String]].javascriptUnbind + """)("path", path0)})
          }
        
        }
      """
    )
  
    // @LINE:10
    def appConfig: JavaScriptReverseRoute = JavaScriptReverseRoute(
      "controllers.Application.appConfig",
      """
        function() {
          return _wA({method:"GET", url:"""" + _prefix + { _defaultPrefix } + """" + "config"})
        }
      """
    )
  
  }


}
