(ns dumpr.row-format
  "Data format for dumpr output")


;; Abstract Data Type for streamed rows
;;
(defn upsert [table id content meta]
  {:pre [(keyword? table) (some? id) (some? meta)]}
  [:upsert table id content meta])

(defn delete [table id content meta]
  {:pre [(keyword? table) (some? id) (some? meta)]}
  [:delete table id content meta])

(defn error [msg ex-data meta]
  [:error msg ex-data meta])

(defn upsert? [data]
  (= (first data) :upsert))

(defn delete? [data]
  (= (first data) :delete))

(defn error? [data]
  (= (first data) :error))
