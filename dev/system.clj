(ns system
  (:require [clojure.core.async :as async :refer [go-loop <!]]
            [com.stuartsierra.component :as component]
            [schema.core :as s]
            [io.aviso.config :as config]
            [dumpr.core :as dumpr]))

(s/defschema LibConf
  "Schema for the library configuration for dev/tests."
  {:conn-params   {(s/optional-key :subname) s/Str
                   :user s/Str
                   :password s/Str
                   :host s/Str
                   :port s/Int
                   :db s/Str
                   :server-id s/Int
                   :stream-keepalive-timeout s/Int
                   :stream-keepalive-interval s/Int
                   :query-max-keepalive-interval s/Int}
   :id-fns        {s/Keyword (s/pred ifn?)}
   :tables        [s/Keyword]
   :filter-tables #{s/Keyword}
   :joplin        {:migrators {s/Keyword s/Str}
                   :databases {s/Keyword {:type s/Keyword
                                          :url s/Str}}
                   :environments {s/Keyword [s/Any]}}})


(defn sink
  "Returns an atom containing a vector. Consumes values from channel
  ch and conj's them into the atom."
  ([ch] (sink ch identity))
  ([ch pr]
   (let [a (atom [])]
     (go-loop []
       (let [val (<! ch)]
         (if-not (nil? val)
           (do
             (pr val)
             (swap! a conj val)
             (recur))
           (pr "Channel closed."))))
     a)))

(defrecord Loader [conf tables]
  component/Lifecycle
    (start [this]
      (if-not (some? (:result this))
        (let [result (dumpr/load-tables tables conf)
              out-rows (sink (:out result))]
          (-> this
              (assoc :result result)
              (assoc :out-rows out-rows)))))

    (stop [this]
      (if (some? (:result this))
        (dissoc this :result)
        this)))

(defn create-loader [conf tables]
  (map->Loader {:conf conf :tables tables}))

(defrecord Stream [conf loader]
  component/Lifecycle
  (start [this]
    (if-not (some? (:stream this))
      (let [binlog-pos (or (:binlog-pos this)
                           (-> loader :result :binlog-pos))
            stream (dumpr/create-stream conf binlog-pos (:filter-tables this))
            out-events (sink (:out stream) println)]
        (dumpr/start-stream stream)
        (-> this
            (assoc :binlog-pos binlog-pos)
            (assoc :out-events out-events)
            (assoc :stream stream)))))

  (stop [this]
    (when (some? (:stream this))
      (dumpr/stop-stream (:stream this)))
    (dissoc this :stream)))

(defn create-stream-continue [conf binlog-pos filter-tables]
  (map->Stream {:conf conf :binlog-pos binlog-pos :filter-tables filter-tables}))

(defn create-stream-new [conf filter-tables]
  (map->Stream {:conf conf :filter-tables filter-tables}))

(defn with-initial-load [config]
  (let [{:keys [conn-params id-fns tables filter-tables]} config
        conf (dumpr/create-conf conn-params id-fns)]
    (component/system-map
     :conf conf
     :loader (create-loader conf tables)
     :streamer (component/using
                (create-stream-new conf filter-tables)
                {:loader :loader}))))

(defn only-stream [config binlog-pos]
  (let [{:keys [conn-params id-fns filter-tables]} config
        conf (dumpr/create-conf conn-params id-fns)]
    (component/system-map
     :conf conf
     :streamer (create-stream-continue conf binlog-pos filter-tables))))
