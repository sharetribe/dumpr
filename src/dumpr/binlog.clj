(ns dumpr.binlog
  (:require [clojure.core.async :as async :refer [chan >!!]]
            [taoensso.timbre :as log]
            [dumpr.query :as query])
  (:import [com.github.shyiko.mysql.binlog
            BinaryLogClient
            BinaryLogClient$EventListener
            BinaryLogClient$LifecycleListener]))


(defn lifecycle-listener [out]
  (reify
    BinaryLogClient$LifecycleListener
    (onConnect [this client]
      (log/info "BinaryLogClient connected"))
    (onCommunicationFailure [this client ex]
      (log/info (str "BinaryLogClient communication failure: " (.getMessage ex))))
    (onEventDeserializationFailure [this client ex]
      (log/info (str "BinaryLogClient event deserialization failure: " (.getMessage ex))))
    (onDisconnect [this client]
      (log/info "BinaryLogClient disconnected")
      (async/close! out))))

(defn event-listener [out]
  (reify
    BinaryLogClient$EventListener
    (onEvent [this payload]
      (>!! out payload))))

(defn new-binlog-client
  "Create a new binary log client for connecting to database using the
  conenction parameters host port user password and server-id. The
  client will start reading binary log at given file and
  position. Events are written to out channel. Out is closed on client
  disconnect."
  [{:keys [host port user password server-id]}
   {:keys [file position]}
   out]
  (doto (BinaryLogClient. host port user password)
    (.setServerId server-id)
    (.setBinlogPosition position)
    (.setBinlogFilename file)
    (.registerEventListener (event-listener out))
    (.registerLifecycleListener (lifecycle-listener out))))

(defn start-client [client]
  (.connect client 1000))

(defn stop-client [client]
  (.disconnect client))
