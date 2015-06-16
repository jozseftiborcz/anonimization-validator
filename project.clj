(defproject anon-valid "0.1.0-SNAPSHOT"
  :description "Validate a database schema via JDBC connection whether it contains sensitive data"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"] 
                 [org.clojure/java.jdbc "0.3.7"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.mchange/c3p0 "0.9.2.1"]
                 [mysql/mysql-connector-java "5.1.6"]]
  :main ^:skip-aot anon-valid.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
