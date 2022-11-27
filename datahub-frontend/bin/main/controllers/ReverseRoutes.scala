// @GENERATOR:play-routes-compiler
// @SOURCE:/root/datahub/datahub-frontend/conf/routes
// @DATE:Fri Nov 18 06:26:01 EST 2022

import play.api.mvc.Call


import _root_.controllers.Assets.Asset

// @LINE:7
package controllers {

  // @LINE:39
  class ReverseTrackingController(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:39
    def track(): Call = {
      
      Call("POST", _prefix + { _defaultPrefix } + "track")
    }
  
  }

  // @LINE:36
  class ReverseAssets(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:36
    def at(file:String): Call = {
      implicit lazy val _rrc = new play.core.routing.ReverseRouteContext(Map(("path", "/public"))); _rrc
      Call("GET", _prefix + { _defaultPrefix } + "assets/" + implicitly[play.api.mvc.PathBindable[String]].unbind("file", file))
    }
  
  }

  // @LINE:15
  class ReverseAuthenticationController(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:16
    def logIn(): Call = {
      
      Call("POST", _prefix + { _defaultPrefix } + "logIn")
    }
  
    // @LINE:17
    def signUp(): Call = {
      
      Call("POST", _prefix + { _defaultPrefix } + "signUp")
    }
  
    // @LINE:15
    def authenticate(): Call = {
      
      Call("GET", _prefix + { _defaultPrefix } + "authenticate")
    }
  
    // @LINE:18
    def resetNativeUserCredentials(): Call = {
      
      Call("POST", _prefix + { _defaultPrefix } + "resetNativeUserCredentials")
    }
  
  }

  // @LINE:19
  class ReverseSsoCallbackController(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:19
    def handleCallback(protocol:String): Call = {
    
      (protocol: @unchecked) match {
      
        // @LINE:19
        case (protocol)  =>
          
          Call("GET", _prefix + { _defaultPrefix } + "callback/" + play.core.routing.dynamicString(implicitly[play.api.mvc.PathBindable[String]].unbind("protocol", protocol)))
      
      }
    
    }
  
  }

  // @LINE:21
  class ReverseCentralLogoutController(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:21
    def executeLogout(): Call = {
      
      Call("GET", _prefix + { _defaultPrefix } + "logOut")
    }
  
  }

  // @LINE:7
  class ReverseApplication(_prefix: => String) {
    def _defaultPrefix: String = {
      if (_prefix.endsWith("/")) "" else "/"
    }

  
    // @LINE:24
    def proxy(path:String): Call = {
    
      (path: @unchecked) match {
      
        // @LINE:24
        case (path)  =>
          
          Call("GET", _prefix + { _defaultPrefix } + "api/" + implicitly[play.api.mvc.PathBindable[String]].unbind("path", path))
      
      }
    
    }
  
    // @LINE:9
    def healthcheck(): Call = {
      
      Call("GET", _prefix + { _defaultPrefix } + "admin")
    }
  
    // @LINE:7
    def index(path:String): Call = {
    
      (path: @unchecked) match {
      
        // @LINE:7
        case (path) if path == "index.html" =>
          implicit lazy val _rrc = new play.core.routing.ReverseRouteContext(Map(("path", "index.html"))); _rrc
          Call("GET", _prefix)
      
        // @LINE:42
        case (path)  =>
          
          Call("GET", _prefix + { _defaultPrefix } + implicitly[play.api.mvc.PathBindable[String]].unbind("path", path))
      
      }
    
    }
  
    // @LINE:10
    def appConfig(): Call = {
      
      Call("GET", _prefix + { _defaultPrefix } + "config")
    }
  
  }


}
