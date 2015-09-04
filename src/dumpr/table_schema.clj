(ns dumpr.table-schema
  "Parsing and manipulating the table schema"
  (:require [schema.core :as s]
            [dumpr.query :as query]))

(def Col
  {:name s/Keyword
   :type s/Keyword
   :character-set (s/maybe s/Str)})

(def TableSchema
  "A schema for parsed table schema"
  {:table s/Keyword
   :primary-key s/Keyword
   :id-fn (s/pred ifn?)
   :cols [Col]}
)

(def TableSpec
  "A partial schema specification for a table with optional id-fn."
  {:table s/Keyword
   (s/optional-key :id-fn) (s/maybe (s/pred ifn?))})

(defn- parse-table-cols
  "Parse the cols column metadata into a table schema presentation."
  [cols]
  (reduce
   (fn [schema {:keys [column_name data_type column_key character_set_name]}]
     (let [name (keyword column_name)
           type (keyword data_type)]
       (if (= column_key "PRI")
         (-> schema
             (update-in [:cols] conj {:name name :type type :character-set character_set_name})
             (assoc :primary-key name))
         (-> schema
             (update-in [:cols] conj {:name name :type type :character-set character_set_name})))))
   {:primary-key nil :cols []}
   cols))

(s/defn load-schema :- TableSchema
  [db-spec :- s/Any
   db :- s/Str
   table-spec :- TableSpec]
  (let [{:keys [table id-fn]} table-spec
        schema-info           (-> (query/fetch-table-cols db-spec db (name table))
                                  parse-table-cols)]
    (-> schema-info
        (assoc :table table)
        (assoc :id-fn (or id-fn
                          (:primary-key schema-info))))))

(s/defn ->table-spec :- TableSpec
  [table :- s/Keyword
   id-fns :- {}]
  {:table table :id-fn (id-fns table)})
