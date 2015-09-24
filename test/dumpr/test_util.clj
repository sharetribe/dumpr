(ns dumpr.test-util
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [<! >! <!! go-loop chan]]
            [joplin.repl :as repl]
            [io.aviso.config :as config]
            [system :refer [LibConf]]))

(defn config []
  (config/assemble-configuration {:prefix "dumpr"
                                  :profiles [:lib :test]
                                  :schemas [LibConf]}))

(defn joplin-config []
  (:joplin (config)))

(defn db-spec []
  {:connection-uri (-> (joplin-config) :databases :sql-test :url)})


(defn reset-test-db
  "Clear the contents of the test db and reset schema."
  []
  (repl/reset (joplin-config) :test :sql-test))

(defn sink-to-coll
  "Sink a channel into a collection. Return a channel that will get a
  single value, the resulting collection, upon in channel being
  closed."
  [in]
  (go-loop [res (transient [])]
    (let [val (<! in)]
      (if-not (nil? val)
        (recur (conj! res val))
        (persistent! res)))))

(defn insert-rows!
  "Insert a seq of [table-name row-content] tuples into test
  database."
  [rows]
  (jdbc/with-db-connection [conn (db-spec)]
    (reduce
     (fn [upserts [table content]]
       (let [res (jdbc/insert! conn table content)
             content-with-id (assoc content
                                    :id
                                    (-> res
                                        first
                                        :generated_key))]
         (conj upserts [:upsert table (:id content-with-id) content-with-id nil])))
     []
     rows)))

(defn ->table-to-rows
  "Convert a seq of upserts into a map of table name to rows"
  [upserts]
  (into {}
        (->> (group-by second upserts)
             (map (fn [[table rows]]
                    [table (map #(nth % 3) rows)])))))
