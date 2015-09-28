(ns dumpr.binlog
  (:require [clojure.core.async :as async :refer [chan >!!]]
            [taoensso.timbre :as log]
            [dumpr.query :as query])
  (:import [com.github.shyiko.mysql.binlog
            BinaryLogClient
            BinaryLogClient$EventListener
            BinaryLogClient$LifecycleListener]))


(defn lifecycle-listener []
  (reify
    BinaryLogClient$LifecycleListener
    (onConnect [this client]
      (log/info "BinaryLogClient connected"))
    (onCommunicationFailure [this client ex]
      (log/warn "BinaryLogClient communication failure: " ex))
    (onEventDeserializationFailure [this client ex]
      (log/warn "BinaryLogClient event deserialization failure: " ex))
    (onDisconnect [this client]
      (log/info "BinaryLogClient disconnected"))))

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
  [{:keys [host
           port
           user
           password
           server-id
           stream-keepalive-interval
           stream-keepalive-timeout]}
   {:keys [file position]}
   out]
  (doto (BinaryLogClient. host port user password)
    (.setServerId server-id)
    (.setBinlogPosition position)
    (.setBinlogFilename file)
    (.setKeepAliveInterval stream-keepalive-interval)
    (.setKeepAliveConnectTimeout stream-keepalive-timeout)
    (.registerEventListener (event-listener out))
    (.registerLifecycleListener (lifecycle-listener))))

;; TODO configurable connection timeout?
(defn start-client [^BinaryLogClient client]
  (.connect client 3000))

(defn stop-client [^BinaryLogClient client]
  (.disconnect client))
