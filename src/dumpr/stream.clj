(ns dumpr.stream
  "Transformations for the stream of parsed binlog events."
  (:require [clojure.core.async :as async :refer [go-loop <! >!]]
            [schema.core :as s]
            [dumpr.table-schema :as table-schema]
            [dumpr.query :as query]
            [dumpr.row-format :as row-format]
            [dumpr.events :as events]))

(defn- preserving-reduced
  [rf]
  #(let [ret (rf %1 %2)]
     (if (reduced? ret)
       (reduced ret)
       ret)))

;; TODO starting streaming in the middle of tx?

(def filter-txs
  "A stateful transducer to filter away canceled
  transactions. Internally batches all events in a transaction and
  releases them at once on successful commit. It removes the events
  marking tx boundaries."
  (fn [rf]
    (let [ongoing? (volatile! false)
          tx       (volatile! [])
          prf      (preserving-reduced rf)
          reset-tx (fn []
                     (let [tx-content @tx]
                       (vreset! ongoing? false)
                       (vreset! tx [])
                       tx-content))
          start-tx (fn [] (vreset! ongoing? true))]

      (fn
        ([] (rf))
        ([result] (rf result))

        ([result input]
         (condp = (events/event-type input)
           :tx-begin (do (start-tx)
                         result)

           :tx-commit (reduce prf result (reset-tx))

           :tx-rollback (do (reset-tx)
                            result)
           (if @ongoing?
             (do (vswap! tx conj input)
                 result)
             (rf result input))))))))

(defn add-binlog-filename [init-filename]
  "Build a stateful transducer to add binlog filename to all
  events. Normally the events data only contains binlog position. This
  transducer tracks the current filename, starting from init-filename,
  and updates the filename from rotate events. Transducer also removes
  the rotate events."
  (fn [rf]
    (let [filename (volatile! init-filename)]
      (fn
        ([] (rf))
        ([result] (rf result))

        ([result input]
         (let [[type data _] input]
           (if (= type :rotate)
             (do (vreset! filename (:filename data))
                 result)
             (rf result (assoc-in input [2 :next-file] @filename)))))))))

(def group-table-maps
  "A stateful transducer to group table-map events with the following
  write/update/delete operation."
  (fn [rf]
    (let [prev (volatile! [::none])]
      (fn
        ([] (rf))
        ([result] (rf result))

        ([result input]
         (let [prior @prev]
           (vreset! prev input)
           (if (= (events/event-type input) :table-map)
             result                           ; Delay table-map events
             (if (= (events/event-type prior) :table-map)
               (rf result [prior input])      ; Return table-map op as pair
               (rf result [input])))))))))    ; op without table map, just wrap

(defn- ->db [event-pair]
  (let [[[event-type event-body]] event-pair]
    (:db event-body)))

(defn- ->table [event-pair]
  (let [[table-map _] event-pair
        event-body (events/event-data table-map)]
    (keyword (:table event-body))))

(defn filter-database
  "Returns a transducer that removes events that are not from the
  given database"
  [expected-db]
  (filter #(= (->db %) expected-db)))

(defn filter-tables
  "Returns a transducer that removes events that are not from the
  given tables. Does not filter events to do not contain
  table (i.e. alter table event)"
  [expected-tables]
  (if (seq expected-tables)
    (filter #(let [table (->table %)]
               (or
                (nil? table)
                (some? (expected-tables table)))))
    (map identity)))

(defn- clear-schema-cache! [schema-cache]
  (reset! schema-cache {}))

(defn- get-from-cache [schema-cache table]
  (@schema-cache (keyword table)))

(defn- add-to-cache! [schema-cache table schema]
  (swap! schema-cache assoc (keyword table) schema))


(defn- validate-schema [schema table-map]
  (let [{:keys [db table]}         (events/event-data table-map)
        meta                       (events/event-meta table-map)
        validation-error (s/check table-schema/TableSchema schema)]
    (if validation-error
      (throw (ex-info "Invalid schema"
                      {:schema schema
                       :db db
                       :table table
                       :meta {:table-map meta :error validation-error}}))
      true)))

(defn- validation-error [schema]
  (s/check table-schema/TableSchema schema))

(defn- valid-schema? [schema]
  (nil? (validation-error schema)))

(defn- fetch-table-schema [table db db-spec id-fns schema-cache]
  (if-let [schema (get-from-cache schema-cache table)]
    schema
    (let [table-spec (table-schema/->table-spec (keyword table) id-fns)
          schema     (table-schema/load-schema db-spec db table-spec)]
      (add-to-cache! schema-cache table schema)
      schema)))

(defn with-table-schema
  [event-pair db-spec id-fns schema-cache]
  (let [[table-map mutation] event-pair]
    (if (= (events/event-type table-map) :table-map)
      (let [{:keys [db table]} (events/event-data table-map)
            schema             (fetch-table-schema table db db-spec id-fns schema-cache)]
        (if (valid-schema? schema)
          [(assoc-in table-map [1 :schema] schema) mutation]
          [(row-format/error "Invalid schema"
                             {:schema schema :db db :table table}
                             {:table-meta (events/event-meta table-map)
                              :error (validation-error schema)})]))
      table-map)))

(defn add-table-schema
  "Processes event-pairs from in channel and adds a schema to the
  table-map event in case the first event is a table-map. Writes
  resulting enriched event-pairs to out channel. If the event-pair
  contains a :alter-table event then that event is filtered and schema
  cache is cleared."
  [out in {:keys [db-spec id-fns schema-cache]}]
  (go-loop []
    (if-some [event-pair (<! in)]
      (let [[table-map _] event-pair]
        (if (= (events/event-type table-map) :alter-table)
          (do (clear-schema-cache! schema-cache)
              (recur))
          (do (>! out (with-table-schema event-pair db-spec id-fns schema-cache))
              (recur))))
      (async/close! out))))

(defn convert-text [col #^bytes val]
  (when val
    (String. val
             (java.nio.charset.Charset/forName (:character-set col)))))

(defmulti convert-type :type)
(defmethod convert-type :tinytext   [col val] (convert-text col val))
(defmethod convert-type :text       [col val] (convert-text col val))
(defmethod convert-type :mediumtext [col val] (convert-text col val))
(defmethod convert-type :longtext   [col val] (convert-text col val))
(defmethod convert-type :default    [col val] val)

(defn- ->name-value [cols row-data]
  (let [cols-names (map :name cols)
        row-data (map convert-type cols row-data)]
    (zipmap cols-names row-data)))

(defn- ->row-format
  [row-data mutation-type table id-fn cols meta]
  (let [mapped-row   (->name-value cols row-data)
        id         (id-fn mapped-row)]
    (if (= :delete mutation-type)
      (row-format/delete table id mapped-row meta)
      (row-format/upsert table id mapped-row meta))))

(defn convert-with-schema
  "Given an event-pair of table-map and mutation (write/update/delete)
  returns a seq of tuples, one per row in mutation, of format
  [row-type table-key id mapped-row meta]. The elements are:
    * row-type - :upsert or :delete
    * table-key - The table name as keyword
    * id - Value of the row primary key
    * mapped-row - Row contents as a map for :upsert, nil for :delete
    * meta - binlog position and ts from the source binlog event"
 [event-pair]
 (let [[table-map mutation] event-pair
       mutation-type (events/event-type mutation)]
    (if (and (= :table-map (events/event-type table-map))
             (#{:write :update :delete} mutation-type))
      (let [table     (-> table-map events/event-data :table keyword)
            schema    (-> table-map events/event-data :schema)
            id-fn     (:id-fn schema)
            cols      (:cols schema)
            rows      (-> mutation events/event-data :rows)
            meta      (events/event-meta mutation)]
        (map
         #(->row-format % mutation-type table id-fn cols meta)
         rows))
      event-pair)))
