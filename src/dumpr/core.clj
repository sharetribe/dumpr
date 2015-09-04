(ns dumpr.core
  "Dumpr API"
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [chan >!!]]
            [schema.core :as s]
            [taoensso.timbre :as log]
            [dumpr.query :as query]
            [dumpr.table-schema :as table-schema]
            [dumpr.events :as events]
            [dumpr.stream :as stream]
            [dumpr.binlog :as binlog]
            [dumpr.row-format :as row-format]))

(def load-buffer-default-size 1000)
(def stream-buffer-default-size 50)



(defn- schema-mapping-ex-handler [^Throwable ex]
  (let [exd  (ex-data ex)
        data (dissoc exd :meta)
        meta (:meta exd)
        msg  (.getMessage ex)]
    (log/error "Error occurred with schema processing:" ex)
    (row-format/error msg data meta)))


;; Public API
;;

(defn create-conf [conn-params id-fns]
  {:db-spec (query/db-spec conn-params)
   :conn-params conn-params
   :id-fns id-fns})

(defn load-tables
  "Load the contents of given tables from the DB and return the
  results as :upsert rows via out channel. Tables are given as a
  vector of table keywords. Keyword is mapped to table name using
  (name table-kw). Loading happens in the order that tables were
  given. Results are returned strictly in the order that tables were
  given. Out channel can be specified if specific buffer size or
  behavior is desired. Result map has following fields:

  :out        The out chan for result rows. Closed when
              all tabels are loaded
  :binlog-pos Binlog position *before* table load started.
              Use this to start binlog consuming."
  ([tables conf] (load-tables tables conf (chan load-buffer-default-size)))
  ([tables {:keys [db-spec conn-params id-fns]} out]
   (let [db          (:db conn-params)
         binlog-pos  (query/binlog-position db-spec)
         tables-ch   (chan 0)
         table-specs (chan 0)]
     (async/onto-chan tables-ch (map #(table-schema/->table-spec % id-fns)
                                     tables))
     (async/pipeline-blocking 1
                              table-specs
                              (map #(s/with-fn-validation (table-schema/load-schema db-spec db %)))
                              tables-ch)
     (async/pipeline-async 1
                           out
                           (partial query/stream-table db-spec)
                           table-specs)
     {:out out
      :binlog-pos binlog-pos})))

(defn binlog-stream
  ([conf binlog-pos] (binlog-stream conf binlog-pos (chan stream-buffer-default-size)))
  ([conf binlog-pos out]
   (let [db-spec      (:db-spec conf)
         id-fns       (:id-fns conf)
         events-xform (comp (map events/parse-event)
                            (remove nil?)
                            stream/filter-txs
                            (stream/add-binlog-filename (:filename binlog-pos))
                            stream/group-table-maps)
         events-ch    (chan 1 events-xform)
         client       (binlog/new-binlog-client (:conn-params conf)
                                         binlog-pos
                                         events-ch)]
     (async/pipeline-blocking 2
                              out
                              (comp (map #(stream/fetch-table-schema db-spec id-fns %))
                                    (map stream/convert-with-schema)
                                    cat)
                              events-ch
                              true
                              schema-mapping-ex-handler)
     {:client client
      :out out})))


(defn start-binlog-stream [stream]
  (binlog/start-client (:client stream)))

(defn close-binlog-stream [stream]
  (binlog/stop-client (:client stream)))
