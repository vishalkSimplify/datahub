// @GENERATOR:play-routes-compiler
// @SOURCE:/root/datahub/datahub-frontend/conf/routes
// @DATE:Mon Oct 10 17:22:23 IST 2022


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
