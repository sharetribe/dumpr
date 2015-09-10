(ns system
  (:require [clojure.core.async :as async :refer [go-loop <!]]
            [com.stuartsierra.component :as component]
            [schema.core :as s]
            [io.aviso.config :as config]
            [dumpr.core :as dumpr]))

(s/defschema LibConf
  "Schema for the library configuration for dev/tests."
  {:conn-params {(s/optional-key :subname) s/Str
                 :user s/Str
                 :password s/Str
                 :host s/Str
                 :port s/Int
                 :db s/Str
                 :server-id s/Int}
   :id-fns {s/Keyword (s/pred ifn?)}
   :tables [s/Keyword]})

(defn config []
  (config/assemble-configuration {:prefix "dumpr"
                                  :profiles [:lib]
                                  :schemas [LibConf]}))

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
                           (-> loader :result :binlog-pos)
                           nil)
            stream (dumpr/binlog-stream conf binlog-pos)
            out-events (sink (:out stream))]
        (dumpr/start-binlog-stream stream)
        (-> this
            (assoc :binlog-pos binlog-pos)
            (assoc :out-events out-events)
            (assoc :stream stream)))))

  (stop [this]
    (when (some? (:stream this))
      (dumpr/close-binlog-stream (:stream this)))
    (dissoc this :stream)))

(defn create-stream-continue [conf binlog-pos]
  (map->Stream {:conf conf :binlog-pos binlog-pos}))

(defn create-stream-new [conf]
  (map->Stream {:conf conf}))

(defn with-initial-load []
  (let [{:keys [conn-params id-fns tables]} (config)
        conf (dumpr/create-conf conn-params id-fns)]
    (component/system-map
     :conf conf
     :loader (create-loader conf tables)
     :streamer (component/using
                (create-stream-new conf)
                {:loader :loader}))))

(defn only-stream [binlog-pos]
  (let [{:keys [conn-params id-fns]} (config)
        conf (dumpr/create-conf conn-params id-fns)]
    (component/system-map
     :conf conf
     :streamer (create-stream-continue conf binlog-pos))))
