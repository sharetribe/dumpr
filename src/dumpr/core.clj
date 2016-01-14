(ns dumpr.core
  "Dumpr API for consuming MySQL database contents as streams of
  updates."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [chan >!!]]
            [schema.core :as s]
            [clojure.tools.logging :as log]
            [manifold.stream]
            [dumpr.query :as query]
            [dumpr.table-schema :as table-schema]
            [dumpr.events :as events]
            [dumpr.stream :as stream]
            [dumpr.binlog :as binlog]
            [dumpr.row-format :as row-format]))

(def #^:private conn-param-defaults
  {:stream-keepalive-interval 60000
   :stream-keepalive-timeout 3000
   :initial-connection-timeout 3000
   :query-max-keepalive-interval 60000})

(defn create-conf
  "Create a common configuration map needed by stream and table load.

  conn-params is a database connection parameters map. Expected keys
  are:

  :user - Database username
  :host - Database host
  :port - Database port
  :db   - The name of the database. You can stream exactly one db.
  :server-id - Unique id in mysql cluster. Dumpr is a replication slave
  :stream-keepalive-interval - Frequency for attempting to
   re-establish a lost connection. Defaults to 1 minute.
  :stream-keepalive-timeout - Timeout for an attempt to restore
   connection, defaults to 3 seconds
  :initial-connection-timeout - Timeout for attempting first connect,
   defaults to 3 seconds.
  :query-max-keepalive-interval - Maximum backoff period between
   failed attempts at loading schema info for a table. Backoff policy
   is exponentially increasing up to this max value. Defaults to 1
   minute.

  (optional) id-fns maps table name (key) to function (value) that
  returns the identifier value for that table row. Normally you'll be
  using the identifier column as a keyword as the id function
  (e.g. {:mytable :identifier}). Using id fn is only required when the
  table doesn't have a single column as primary key that could be
  autodetected, or when you wish to construct id differently on
  purpose.

  (optional) db-spec is passed to JDBC when querying database. This is
  optional and should normally be left out. By default db-spec is
  built from conn-params but it can be explicitly specified to use
  e.g. a connection pool."
  ([conn-params] (create-conf conn-params {} nil))
  ([conn-params id-fns] (create-conf conn-params id-fns nil))
  ([conn-params id-fns db-spec]
   {:db-spec     (or db-spec (query/db-spec conn-params))
    :conn-params (merge conn-param-defaults conn-params)
    :id-fns      (or id-fns {})}))


(def load-buffer-default-size 1000)

(defn create-table-stream
  "Creates a stream that, when started via start-stream, will load the
  contents of given tables from the DB. The output of the stream can
  be consumed as a Manifold source using (source stream).

  Tables are given as a vector of table keywords. Keyword is mapped to
  table name using (name table-kw). Loading happens in the order that
  tables were given. Results are returned strictly in the order that
  tables were given.

  Policy for out channel can be specified if specific buffer size (as
  long) or behavior (as core.async channel) is desired. Note that the
  results are always made available as a Manifold source despite the
  possible out channel provided.

  The table stream also supports getting tbe binary log position to
  continue streaming from. This is made available via (next-position
  stream). This position is recorded when the stream is
  started. Calling enxt-position before that returns nil."
  ([conf tables]
   (create-table-stream conf tables (chan load-buffer-default-size)))
  ([conf tables out-ch]
   (stream/new-table-load-stream tables conf out-ch)))

(defn next-position
  "Return the binlog position to use when continuing streaming after
  the given initial stream. This only works for a finite stream, i.e.
  the table load stream and is available only after calling
  start-stream on stream."
  [stream]
  (stream/next-position stream))

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

(defn binlog-position
  "Query the most recent binary log position. This function can also
  be used as a way to test the connection to database by checking the
  possible raised exception.

  Returns the binlog position as {:file \"filename\" :position 123}"
  [conf]
  (let [db-spec (:db-spec conf)]
    (query/binlog-position db-spec)))


(defn- validate-binlog-pos! [conf binlog-pos]
  (when-not (valid-binlog-pos? conf binlog-pos)
    (throw (ex-info "Invalid binary log position."
                    {:binlog-pos binlog-pos}))))

(def stream-buffer-default-size 50)

(defn create-binlog-stream
  "Create a new binlog stream using the given conf that will start
  streaming from binlog-pos position. Optional parameters are:

  only-tables - Only stream events from this set of tables
  out - Output buffer policy to use (buffer size as long or a
        core.async channel). Defaults to a small blocking buffer where
        consumption is expected to keep up with the stream volume.

  Returns a binlog stream instance. Use start-stream and stop-stream
  to start and stop streaming. Use source to retrieve a Manifold
  source to consume stream output. Return type being a record is an
  implementation detail and subject to can change."
  ([conf binlog-pos]
   (create-binlog-stream conf binlog-pos nil (chan stream-buffer-default-size)))

  ([conf binlog-pos only-tables]
   (create-binlog-stream conf binlog-pos only-tables (chan stream-buffer-default-size)))

  ([conf binlog-pos only-tables out-ch]
   (validate-binlog-pos! conf binlog-pos)
   (stream/new-binlog-stream conf binlog-pos only-tables out-ch)))


(defn start-stream!
  "Start streaming. Return `true` when streaming is started. Returns
  `nil` if streaming was already started before and nothing was done."
  [stream]
  (stream/start! stream))

(defn stop-stream!
  "Stop the stream. A stream once stopped cannot be restarted. Return
  `true` when streaming is stopped. Returns `nil` if streaming was
  already stopped before and nothing was done."
  [stream]
  (stream/stop! stream))

(defn source
  "Get the stream output as a Manifold source."
  [stream]
  (stream/source stream))
