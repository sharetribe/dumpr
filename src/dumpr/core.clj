(ns dumpr.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [go >! chan]]
            [taoensso.timbre :as log]))

(def ^:dynamic *parallel-table-loads* 2)
(def buffer-default-size 1000)

;; Abstract Data Type for streamed rows
;;
(defn upsert [table content]
  [:upsert table content])

(defn delete [table id]
  [:delete table id])

(defn upsert? [data]
  (= (first data) :upsert))

(defn delete? [data]
  (= (first data) :delete))


(defn- query-binlog-position [db-spec]
  (first (jdbc/query db-spec ["SHOW MASTER STATUS"])))


(defn- stream-table [db-spec table ch]
  (async/thread
    (let [count (jdbc/query
                 db-spec
                 [(str "SELECT * FROM " (name table))]
                 :row-fn (fn [v]
                           ;; Block until output written to make sure
                           ;; we don't close DB connection too early.
                           (async/>!! ch (upsert table v))
                           1)
                 :result-set-fn (partial reduce + 0))]
      (log/info "Loaded" count "rows from table" table))
    (async/close! ch)))


(defn create [db-spec tables]
  {:db-spec db-spec :tables tables})

(defn load-tables
  ([ctx] (load-tables ctx (chan buffer-default-size)))
  ([ctx out]
   (let [{:keys [db-spec tables]} ctx
         binlog-position          (query-binlog-position db-spec)
         in (chan)
         _ (async/onto-chan in tables)
         _ (async/pipeline-async *parallel-table-loads*
                                 out
                                 (partial stream-table db-spec)
                                 in)]
     {:out out
      :binlog-position binlog-position})))

