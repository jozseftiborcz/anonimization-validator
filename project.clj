(defproject anon-valid "0.1.0-SNAPSHOT"
  :description "Validate a database schema via JDBC connection whether it contains sensitive data"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"] 
                 [org.clojure/java.jdbc "0.3.7"]
                 [org.clojure/tools.cli "0.3.2"]
                 [clojure.jdbc/clojure.jdbc-c3p0 "0.3.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [bultitude "0.2.8"]
                 [clojure-term-colors "0.1.0-SNAPSHOT"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [com.mchange/c3p0 "0.9.5"]
                 [mysql/mysql-connector-java "5.1.6"]]
  :main ^:skip-aot anon-valid.main
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
