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


;; Public API
;;

(defn create-conf
  "Create configuration needed by stream and table load.
  Takes two params:

  :conn-params Map that contains the following keys:
               :user, :password, :host, :port, :db and :server-id
  :id-fns      Maps table name (key) to function (value) that returns the
               identifier value for that table row. Normally you'll be using
               the identifier column as a keyword as the id function
               (e.g. {:mytable :identifier})"
  [conn-params id-fns]
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
         table-specs (chan 0)
         schema-chan (table-schema/load-and-parse-schemas
                      tables db-spec db id-fns)]

     (async/pipeline-async 1
                           out
                           (partial query/stream-table db-spec)
                           schema-chan)
     {:out out
      :binlog-pos binlog-pos})))

(defn binlog-stream
  ([conf binlog-pos] (binlog-stream conf binlog-pos nil (chan stream-buffer-default-size)))
  ([conf binlog-pos tables] (binlog-stream conf binlog-pos tables (chan stream-buffer-default-size)))
  ([conf binlog-pos tables out]
   (let [db-spec            (:db-spec conf)
         db                 (get-in conf [:conn-params :db])
         id-fns             (:id-fns conf)
         keepalive-interval (:query-keepalive-max-interval conf)
         schema-cache       (atom {})
         events-xform       (comp (map events/parse-event)
                                  (remove nil?)
                                  stream/filter-txs
                                  (stream/add-binlog-filename (:filename binlog-pos))
                                  stream/group-table-maps
                                  (stream/filter-database db)
                                  (stream/filter-tables (set tables)))
         events-ch          (chan 1 events-xform)
         schema-loaded-ch   (chan 1)
         client             (binlog/new-binlog-client (:conn-params conf)
                                                      binlog-pos
                                                      events-ch)]

     (stream/add-table-schema schema-loaded-ch
                              events-ch
                              {:schema-cache schema-cache
                               :db-spec db-spec
                               :id-fns id-fns
                               :keepalive-interval keepalive-interval
                               })
     (async/pipeline 4
                     out
                     (comp (map stream/convert-with-schema)
                           cat)
                     schema-loaded-ch)
     {:client client
      :out out})))


(defn start-binlog-stream [stream]
  (binlog/start-client (:client stream)))

(defn close-binlog-stream [stream]
  (binlog/stop-client (:client stream)))
