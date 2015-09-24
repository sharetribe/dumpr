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


(defn reset-test-db!
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

(defn into-test-db!
  "Interpret the given ordered ops sequence as SQL inserts, updates
  and deletes and run them against the test DB."
  [ops]
  (jdbc/with-db-connection [conn (db-spec)]
    (reduce
     (fn [res [type table id content _]]
       (conj res
             (condp = type
               :insert (jdbc/insert! conn table content)
               :update (jdbc/update! conn table content ["id = ?" id])
               :delete (jdbc/delete! conn table ["id = ?" id]))))
     []
     ops)))

(defn table-id-key [[_ table id _ _]]
  (str table "-" id))

(defn into-entity-map
  "Interpret the given ordered ops sequence by building a map of the
  final values for entities, identified by table and id. Insert and
  updates add entities into map and deletes remove them."
  [ops]
  (reduce (fn [entities [type table id content _ :as op]]
            (let [key (table-id-key op)]
              (condp = type
                :insert (assoc entities key content)
                :update (assoc entities key content)
                :upsert (assoc entities key content)
                :delete (dissoc entities key))))
          {}
          ops))

