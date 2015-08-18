(defproject dumpr "0.1.0-SNAPSHOT"
  :description "Library to consume MySQL contents as a stream of updates."
  :url "FIXME"

  :source-paths ["src"]

  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [mysql/mysql-connector-java "5.1.36"]
                 [org.clojure/java.jdbc "0.4.1"]
                 [clj-time "0.11.0"]
                 [com.taoensso/timbre "4.1.1"]
                 [com.github.shyiko/mysql-binlog-connector-java "0.2.2"]]

  :min-lein-version "2.5.0"

  :uberjar-name "dumpr.jar"

  :clean-targets ^{:protect false} ["./target"]

  :profiles {:uberjar {:aot :all}
             :dev {:source-paths ["dev"]}})
