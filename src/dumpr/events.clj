(ns dumpr.events
  "Parsing of native Binlog client event types to clojure data."
  (:import [com.github.shyiko.mysql.binlog.event
            EventType
            EventHeaderV4
            Event
            TableMapEventData
            DeleteRowsEventData
            WriteRowsEventData
            UpdateRowsEventData
            RotateEventData
            QueryEventData]))

(def event-mappings
  {EventType/UNKNOWN            ::ev-unknown
   EventType/START_V3           ::ev-start-v3
   EventType/QUERY              ::ev-query
   EventType/STOP               ::ev-stop
   EventType/ROTATE             ::ev-rotate
   EventType/INTVAR             ::ev-intvar
   EventType/LOAD               ::ev-load
   EventType/SLAVE              ::ev-slave
   EventType/CREATE_FILE        ::ev-create-file
   EventType/APPEND_BLOCK       ::ev-append-block
   EventType/EXEC_LOAD          ::ev-exec-load
   EventType/DELETE_FILE        ::ev-delete-file
   EventType/NEW_LOAD           ::ev-new-load
   EventType/RAND               ::ev-rand
   EventType/USER_VAR           ::ev-user-var
   EventType/FORMAT_DESCRIPTION ::ev-format-description
   EventType/XID                ::ev-xid
   EventType/BEGIN_LOAD_QUERY   ::ev-begin-load-query
   EventType/EXECUTE_LOAD_QUERY ::ev-execute-load-query
   EventType/TABLE_MAP          ::ev-table-map
   EventType/PRE_GA_WRITE_ROWS  ::ev-pre-ga-write-rows
   EventType/PRE_GA_UPDATE_ROWS ::ev-pre-ga-update-rows
   EventType/PRE_GA_DELETE_ROWS ::ev-pre-ga-delete-rows
   EventType/WRITE_ROWS         ::ev-write-rows
   EventType/UPDATE_ROWS        ::ev-update-rows
   EventType/DELETE_ROWS        ::ev-delete-rows
   EventType/INCIDENT           ::ev-incident
   EventType/HEARTBEAT          ::ev-heartbeat
   EventType/IGNORABLE          ::ev-ignorable
   EventType/ROWS_QUERY         ::ev-rows-query
   EventType/EXT_WRITE_ROWS     ::ev-ext-write-rows
   EventType/EXT_UPDATE_ROWS    ::ev-ext-update-rows
   EventType/EXT_DELETE_ROWS    ::ev-ext-delete-rows
   EventType/GTID               ::ev-gtid
   EventType/ANONYMOUS_GTID     ::ev-anonymous-gtid
   EventType/PREVIOUS_GTIDS     ::ev-previous-gtids})

(defn header-parser [^EventHeaderV4 header]
  {:ts (-> header .getTimestamp java.util.Date.)
   :next-position (.getNextPosition header)})

(defn event-parser
  "Build an event parser from the given body parser by adding the
  standard header parsing functionality. Body parser must return a
  tuple of [parsed-event-type parsed-event-data] or nil. In case of
  nil the event is to be ignored and nil is returned from the final
  parser.

  The final return type of an event-parser is:
  [parsed-event-type parsed-body parsed-header] or nil."
  [body-parser]
  (fn [^Event payload]
    (let [header      (header-parser (.getHeader payload))
          body        (body-parser (.getData payload))
          [type data] body]
      (when body
        [type data header]))))


;; Body parsers for interesting event types
;;

(defn rotate-parser [^RotateEventData data]
  [:rotate {:filename (.getBinlogFilename data)
            :position (.getBinlogPosition data)}])

(defn query-parser [^QueryEventData data]
  (let [sql        (.getSql data)
        event-data {:db (.getDatabase data)}]
    (condp re-find (.toUpperCase sql)
      #"^BEGIN"       [:tx-begin event-data]
      #"^ROLLBACK"    [:tx-rollback event-data]
      #"^COMMIT"      [:tx-commit event-data]
      #"^ALTER TABLE" [:alter-table event-data]
      nil)))

(defn xid-parser [data]
  [:tx-commit nil])

(defn table-map-parser [^TableMapEventData data]
  [:table-map {:table-id (.getTableId data)
               :db       (.getDatabase data)
               :table    (.getTable data)}])


(defn update-parser [^UpdateRowsEventData data]
  [:update {:table-id (.getTableId data)
            :rows     (into [] (for [[_ v] (.getRows data)] (into [] v)))}])

(defn write-parser [^WriteRowsEventData data]
  [:write {:table-id (.getTableId data)
           :rows (into [] (map (partial into []) (.getRows data)))}])

(defn delete-parser [^DeleteRowsEventData data]
  [:delete {:table-id (.getTableId data)
            :rows (into [] (map (partial into []) (.getRows data)))}])

(defn stop-parser [data]
  [:stop nil])

(def event-parsers
  (let [rotate    (event-parser rotate-parser)
        query     (event-parser query-parser)
        table-map (event-parser table-map-parser)
        update    (event-parser update-parser)
        write     (event-parser write-parser)
        delete    (event-parser delete-parser)
        xid       (event-parser xid-parser)
        stop      (event-parser stop-parser)]
    {::ev-query query
     ::ev-table-map table-map
     ::ev-xid xid
     ::ev-rotate rotate
     ::ev-stop stop
     ::ev-pre-ga-write-rows write
     ::ev-ext-write-rows write
     ::ev-write-rows write
     ::ev-pre-ga-update-rows update
     ::ev-ext-update-rows update
     ::ev-update-rows update
     ::ev-pre-ga-delete-rows delete
     ::ev-ext-delete-rows delete
     ::ev-delete-rows delete}))


(defn parse-event [^Event payload]
  "Parse native Binlog client event to Clojure data. Returns nil if
  the event has no parsing logic defined."
  (when-let [parser (-> payload
                        .getHeader
                        .getEventType
                        event-mappings
                        event-parsers)]
    (parser payload)))

(defn event-type
  "Extract the event type keyword from event."
  [event]
  (first event))

(defn event-meta
  "Extract the metadata part of event."
  [event]
  (nth event 2))

(defn event-data
  "Extract the data part of event."
  [event]
  (second event))
