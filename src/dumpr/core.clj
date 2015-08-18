(ns dumpr.core
  (:require [clojure.java.jdbc :as jdbc]))

(defn- query-binlog-position [db-spec]
  (first (jdbc/query db-spec ["SHOW MASTER STATUS"])))


(defn create [db-spec tables]
  {:db-spec db-spec :tables tables})

(defn load-tables [ctx]
  (let [{:keys [db-spec tables]} ctx]
    {:event nil
     :binlog-position (query-binlog-position db-spec)}))

