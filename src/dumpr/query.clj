(ns dumpr.query
  "Functions to query data from MySQL and parse the query results."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [>!!]]
            [taoensso.timbre :as log]))


(defn db-spec
  "Build a db spec from configuration for connecting with database."
  [{:keys [host port db user password subname]}]
  {:subprotocol "mysql"
   :subname     (or subname
                    (str "//" host ":" port "/" db "?zeroDateTimeBehavior=convertToNull"))
   :user        user
   :password    password})

(defn binlog-position
  "Query binary log position from MySQL."
  [db-spec]
  (first (jdbc/query db-spec ["SHOW MASTER STATUS"])))

(defn stream-table
  "Stream the contents of a given database table to a core.async
  channel. Designed to work as async-fn of
  clojure.core.async/pipeline-async meaning that takes output channel
  as last argument and closes the channel after streaming is
  complete.."
  [db-spec {:keys [table id-fn]} ch]
  (async/thread
    (log/info "Starting data load from table" table)
    (let [count (jdbc/query
                 db-spec
                 [(str "SELECT * FROM " (name table))]
                 :row-fn (fn [v]
                           ;; Block until output written to make sure
                           ;; we don't close DB connection too early.
                           (>!! ch [table (id-fn v) v])
                           1)
                 :result-set-fn (partial reduce + 0))]
      (log/info "Loaded" count "rows from table" table))
    (async/close! ch)))
