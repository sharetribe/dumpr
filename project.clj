(defproject org.sharetribe/dumpr "0.1.4-SNAPSHOT"
  :description "Live replicate data from a MySQL database to your own process"
  :url "https://github.com/sharetribe/dumpr"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :source-paths ["src"]

  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.logging "0.3.1"]
                 [mysql/mysql-connector-java "5.1.38"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [prismatic/schema "1.0.5"]
                 [com.github.shyiko/mysql-binlog-connector-java "0.3.1"]
                 [manifold "0.1.2"]]

  :global-vars {*warn-on-reflection* true}
  :min-lein-version "2.5.0"

  :profiles {:dev {:source-paths ["config" "dev"]
                   :dependencies [[com.stuartsierra/component "0.2.3"]
                                  [reloaded.repl "0.2.0"]
                                  [io.aviso/config "0.1.7"]
                                  [org.clojure/test.check "0.8.2"]
                                  [com.gfredericks/test.chuck "0.2.0"]]}})
