(ns dumpr.query
  "Functions to query data from MySQL and parse the query results."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [>!!]]
            [taoensso.timbre :as log]
            [dumpr.row-format :as row-format]))


(defn db-spec
  "Build a db spec from configuration for connecting with database."
  [{:keys [host port db user password subname]}]
  {:subprotocol "mysql"
   :subname     (or subname
                    (str "//" host ":" port "/" db "?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"))
   :user        user
   :password    password})

(defn binlog-position
  "Query binary log position from MySQL."
  [db-spec]
  (-> (jdbc/query db-spec ["SHOW MASTER STATUS"])
      first
      (select-keys [:file :position])
      (update :position long)))

(defn show-binlog-positions
  "List all available binary log positions."
  [db-spec]
  (jdbc/query db-spec ["SHOW BINARY LOGS"]))

(defn stream-table
  "Stream the contents of a given database table to a core.async
  channel. Designed to work as async-fn of
  clojure.core.async/pipeline-async meaning that takes output channel
  as last argument and closes the channel after streaming is
  complete.."
  [db-spec {:keys [table id-fn]} ch]
  (async/thread
    (log/info "Starting data load from table" table "id-fn:" id-fn)
    (let [count (jdbc/query
                 db-spec
                 [(str "SELECT * FROM " (name table))]
                 :row-fn (fn [v]
                           ;; Block until output written to make sure
                           ;; we don't close DB connection too early.
                           (>!! ch (row-format/upsert table (id-fn v) v nil))
                           1)
                 :result-set-fn (partial reduce + 0))]
      (log/info "Loaded" count "rows from table" table))
    (async/close! ch)))

(defn fetch-table-cols
  "Query table column metadata for db and table."
  [db-spec db table]
  (jdbc/query
   db-spec
   ["SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, CHARACTER_SET_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
    db
    table]))

(defn parse-table-schema
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
