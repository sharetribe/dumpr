(ns dumpr.core
  "Dumpr API"
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [chan >!!]]
            [schema.core :as s]
            [taoensso.timbre :as log]
            [manifold.stream]
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

(def #^:private conn-param-defaults
  {:stream-keepalive-interval 60000
   :stream-keepalive-timeout 3000
   :initial-connection-timeout 3000
   :query-max-keepalive-interval 60000})

(defn create-conf
  "Create a common configuration needed by stream and table load.
  Takes two params:

  :conn-params Database connection parameters map. Expected keys are
    :user - Database username
    :host - Database host
    :port - Database port
    :db   - The name of the database. You can stream exactly one db.
    :server-id - Unique id in mysql cluster. Dumpr is a replication slave
    :stream-keepalive-interval - Frequency for attempting to
     re-establish a lost connection. Defaults to 1 minute.
    :stream-keepalive-timeout - Timeout for an attempt to restore
     connection, defaults to 3 seconds
    :initial-connection-timeout - Timeout for attempting first
     connect, defaults to 3 seconds.
    :query-max-keepalive-interval - Maximum backoff period between
     failed attempts at loading schema info for a table. Backoff
     policy is exponentially increasing up to this max value. Defaults
     to 1 minute.

  :id-fns      Maps table name (key) to function (value) that returns the
               identifier value for that table row. Normally you'll be using
               the identifier column as a keyword as the id function
               (e.g. {:mytable :identifier})

  :db-spec     The db-spec passed to JDBC when querying database. This is
               optional and should normally be left out. By default
               db-spec is built from conn-params but it can be
               overridden to use a connection pool."
  ([conn-params id-fns] (create-conf conn-params id-fns nil))
  ([conn-params id-fns db-spec]
   {:db-spec     (or db-spec (query/db-spec conn-params))
    :conn-params (merge conn-param-defaults conn-params)
    :id-fns      id-fns}))

(defn- ->connected-source [ch]
  (let [stream (manifold.stream/stream)]
    (manifold.stream/connect ch stream)
    stream))

(defn load-tables
  "Load the contents of given tables from the DB and return the
  results as :upsert rows via out channel. Tables are given as a
  vector of table keywords. Keyword is mapped to table name using
  (name table-kw). Loading happens in the order that tables were
  given. Results are returned strictly in the order that tables were
  given. Policy for out channel can be specified if specific buffer
  size (as long) or behavior (as core.async channel) is
  desired. Note that the return value is always a Manifold source
  despite the possible out channel you provide. Result map has
  following fields:

  :out        A Manifold source for result rows. Drained when
              all tabels are loaded
  :binlog-pos Binlog position *before* table load started.
              Use this to start binlog consuming."
  ([tables conf] (load-tables tables conf (chan load-buffer-default-size)))
  ([tables {:keys [db-spec conn-params id-fns]} out-ch]
   (let [db          (:db conn-params)
         binlog-pos  (query/binlog-position db-spec)
         tables-ch   (chan 0)
         table-specs (chan 0)
         schema-chan (table-schema/load-and-parse-schemas
                      tables db-spec db id-fns)]

     (async/pipeline-async 1
                           out-ch
                           (partial query/stream-table db-spec)
                           schema-chan)
     {:out (->connected-source out-ch)
      :binlog-pos binlog-pos})))

(defn valid-binlog-pos?
  "Validate that the given binlog position is available at the DB
  server and streaming can be started from this position.

  Note! The validation is not perfect. It will check the given
  position against available files and their max positions but cannot
  tell if a position is in the middle of an event. In practice this
  never occurs when the continue position is fetched from an event
  produced by the lib."
  [conf binlog-pos]
  (let [db-spec                 (:db-spec conf)
        {:keys [file position]} binlog-pos
        valid-positions         (query/show-binlog-positions db-spec)]
    (->> valid-positions
         (some (fn [{:keys [log_name file_size]}]
                 (and (= log_name file)
                      (<= position file_size))))
         boolean)))

(defn- validate-binlog-pos! [conf binlog-pos]
  (when-not (valid-binlog-pos? conf binlog-pos)
    (throw (ex-info "Invalid binary log position."
                    {:binlog-pos binlog-pos}))))


(defn create-binlog-stream
  "Create a new binlog stream using the given conf that will start
  streaming from binlog-pos position. Optional parameters are:

  only-tables - Only stream events from this set of tables
  out - Output buffer policy to use (buffer size as long or a
        core.async channel). Defaults to a small blocking buffer where
        consumption is expected to keep up with the stream volume.

  Returns a binlog stream instance. Use start and stop to start and
  stop streaming. Use source to retrieve a Manifold source to consume
  stream output. Return type being a record is an implementation
  detail and subject to can change."
  ([conf binlog-pos]
   (create-binlog-stream conf binlog-pos nil (chan stream-buffer-default-size)))

  ([conf binlog-pos only-tables]
   (create-binlog-stream conf binlog-pos only-tables (chan stream-buffer-default-size)))

  ([conf binlog-pos only-tables out-ch]
   (validate-binlog-pos! conf binlog-pos)
   (stream/new-binlog-stream conf binlog-pos only-tables out-ch)))


(defn start-stream
  "Start streaming"
  [stream]
  (stream/start! stream))

(defn stop-stream
  "Stop the stream. A stream once stopped cannot be restarted."
  [stream]
  (stream/stop! stream))

(defn source
  "Get a Manifold source of the stream output."
  [stream]
  (stream/source stream))
