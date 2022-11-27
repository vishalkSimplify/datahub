// @GENERATOR:play-routes-compiler
// @SOURCE:/root/datahub/datahub-frontend/conf/routes
// @DATE:Fri Nov 18 06:26:01 EST 2022


package router {
  object RoutesPrefix {
    private var _prefix: String = "/"
    def setPrefix(p: String): Unit = {
      _prefix = p
    }
    def prefix: String = _prefix
    val byNamePrefix: Function0[String] = { () => prefix }
  }
}
