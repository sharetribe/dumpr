(ns dumpr.stream
  "Transformations for the stream of parsed binlog events."
  (:require [clojure.core.async :as async]
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


(defn- validate-schema [schema table-map]
  (let [{:keys [primary-key cols]} schema
        {:keys [db table]}         (events/event-data table-map)
        meta                       (events/event-meta table-map)]
    (if-not (keyword? primary-key)
      (throw
       (ex-info "Invalid schema. Missing primary-key"
                {:schema schema :db db :table table :meta meta}))
      (if (empty? cols)
        (throw (ex-info "Invalid schema. No columns."
                        {:schema schema :db db :table table :meta meta}))
        true))))

(defn- load-schema-from-db [db-spec table-map]
  (let [{:keys [db table]} (events/event-data table-map)
        cols               (query/fetch-table-cols db-spec db table)]
    (query/parse-table-schema cols)))

;; TODO Table schema caching
(defn fetch-table-schema [db-spec id-fns event-pair]
  (let [[table-map mutation] event-pair]
    (if (= (events/event-type table-map) :table-map)
      (let [{:keys [db table]} (events/event-data table-map)
            table-spec (table-schema/->table-spec (keyword table) id-fns)
            schema (table-schema/load-schema db-spec db table-spec)
            validation-error (s/check table-schema/TableSchema schema)]
        (if validation-error
          (throw (ex-info "Invalid schema"
                          {:schema schema
                           :db db
                           :table table
                           :meta {:table-map (events/event-meta table-map) :error validation-error}}))
          [(assoc-in table-map [1 :schema] schema) mutation]))
      table-map)))

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
      (throw (ex-info
              (str "Expected event-pair starting with :table-map, got: "
                   (events/event-type table-map))
              :event-pair event-pair
              :meta (-> event-pair first (nth 2)))))))
