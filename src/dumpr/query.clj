(ns dumpr.query
  "Functions to query data from MySQL and parse the query results."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as string]
            [clojure.core.async :as async :refer [>!!]]
            [clojure.tools.logging :as log]
            [dumpr.row-format :as row-format])
  (:import (java.time LocalDateTime ZoneOffset)))


(defn db-spec
  "Build a db spec from configuration for connecting with database."
  [{:keys [host port db user password subname]}]
  {:subprotocol "mysql"
   :subname     (or subname
                    (str "//" host ":" port "/" db "?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&connectionTimeZone=SERVER&preserveInstants=true&useSSL=false"))
   :user        user
   :password    password})

(defn- parse-mysql-version
  [version-str]
  (let [numeric (first (string/split version-str #"-"))]
    (mapv #(Long/parseLong %) (string/split numeric #"\."))))

(defn- mysql-version>=
  [[maj1 min1] [maj2 min2]]
  (or (> maj1 maj2)
      (and (= maj1 maj2) (>= min1 min2))))

(defn- binlog-status-query
  "Return the appropriate SQL query for binlog position based on
  MySQL server version. MySQL 8.4+ replaced SHOW MASTER STATUS with
  SHOW BINARY LOG STATUS."
  [db-spec]
  (let [version-str (-> (jdbc/query db-spec ["SELECT VERSION() AS version"])
                        first
                        :version)
        version (parse-mysql-version version-str)]
    (if (mysql-version>= version [8 4])
      "SHOW BINARY LOG STATUS"
      "SHOW MASTER STATUS")))

(defn binlog-position
  "Query binary log position from MySQL."
  [db-spec]
  (-> (jdbc/query db-spec [(binlog-status-query db-spec)])
      first
      (select-keys [:file :position])
      (update :position long)))

(defn show-binlog-positions
  "List all available binary log positions."
  [db-spec]
  (jdbc/query db-spec ["SHOW BINARY LOGS"]))

(defn- convert-timestamps
  [row]
  (into {}
        (map (fn [[k v]]
               (if (instance? LocalDateTime v)
                 ;; Converting with hard-coded UTC timezone is ok and matches
                 ;; previous behavior. The LocalDateTime value is already
                 ;; possibly converted by the MySQL connector, depending on the
                 ;; connection configuration.
                 [k (java.util.Date/from (.toInstant ^LocalDateTime v ZoneOffset/UTC))]
                 [k v])))
        row))

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
                 {:row-fn (fn [v]
                            ;; Block until output written to make sure
                            ;; we don't close DB connection too early.
                            (>!! ch (row-format/upsert table (id-fn v) (convert-timestamps v) nil))
                            1)
                  :result-set-fn (partial reduce + 0)})]
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
