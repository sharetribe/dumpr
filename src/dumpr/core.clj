(ns dumpr.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [chan >!!]]
            [taoensso.timbre :as log]
            [dumpr.query :as query]
            [dumpr.events :as events]
            [dumpr.stream :as stream])
  (:import [com.github.shyiko.mysql.binlog
            BinaryLogClient
            BinaryLogClient$EventListener
            BinaryLogClient$LifecycleListener]
           [com.github.shyiko.mysql.binlog.event EventType]))

(def ^:dynamic *parallel-table-loads* 2)
(def load-buffer-default-size 1000)
(def stream-buffer-default-size 50)

;; Abstract Data Type for streamed rows
;;
(defn upsert [table id content]
  [:upsert table id content])

(defn delete [table id]
  [:delete table id])

(defn upsert? [data]
  (= (first data) :upsert))

(defn delete? [data]
  (= (first data) :delete))



;; Initial table load stuff...
;;


(defn- ensure-table-spec [name-or-spec]
  (if (keyword? name-or-spec)
    {:table name-or-spec
     :id-fn :id}
    name-or-spec))



;; Binlog streaming stuff...
;;

(comment
  (defn fetch-table-cols [db-spec db table]
    (jdbc/query
     db-spec
     ["SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
      db
      (name table)]))

  (defn parse-table-schema [cols table]
    (reduce
     (fn [schema {:keys [column_name data_type column_key]}]
       (let [name (keyword column_name)
             type (keyword data_type)]
         (if (= column_key "PRI")
           (-> schema
               (update-in [:cols] conj {:name name :type type})
               (assoc :primary_key name))
           (-> schema
               (update-in [:cols] conj {:name name :type type})))))
     {:table table :primary_key nil :cols []}
     cols)))



(defonce events (atom []))

(defn- lifecycle-listener [out]
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

(defn- event-listener [out]
  (reify
    BinaryLogClient$EventListener
    (onEvent [this payload]
      (swap! events conj payload)
      (>!! out payload))))

(defn- new-binlog-client [conf binlog-pos out & {:keys [filter-types]}]
  (let [{:keys [host port user password server-id]} conf
        {:keys [file position]}                     binlog-pos]
    (doto (BinaryLogClient. host port user password)
      (.setServerId server-id)
      (.setBinlogPosition position)
      (.setBinlogFilename file)
      (.registerEventListener (event-listener out))
      (.registerLifecycleListener (lifecycle-listener out)))))


;; Public API
;;

(defn create [conf tables]
  {:db-spec (query/db-spec conf) :conf conf :tables tables})

(defn load-tables
  ([ctx] (load-tables ctx (chan load-buffer-default-size)))
  ([{:keys [db-spec tables]} out]
   (let [binlog-pos (query/binlog-position db-spec)
         in         (chan 0)
         _          (async/pipeline-async *parallel-table-loads*
                                          out
                                          (partial query/stream-table db-spec) ; TODO upserts
                                          in)
         _          (async/onto-chan in (map ensure-table-spec tables))]
     {:out out
      :binlog-pos binlog-pos})))

(defn stream-binlog
  ([ctx binlog-pos] (stream-binlog ctx binlog-pos (chan stream-buffer-default-size)))
  ([ctx binlog-pos out]
   (let [events-xform (comp (map events/parse-event)
                            (remove nil?)
                            stream/filter-txs
                            (stream/add-binlog-filename (:filename binlog-pos))
                            stream/group-table-maps)
         events-ch    (chan 1 events-xform)
         client       (new-binlog-client (:conf ctx)
                                         binlog-pos
                                         events-ch)]
     {:client client
      :out events-ch})))

