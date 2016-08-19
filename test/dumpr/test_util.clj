(ns dumpr.test-util
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [<! >! <!! go-loop chan]]
            [io.aviso.config :as config]
            [manifold.stream]
            [system :refer [LibConf]]))

(defn config []
  (config/assemble-configuration {:prefix "dumpr"
                                  :profiles [:lib :test]
                                  :schemas [LibConf]}))

(defn db-spec []
  {:connection-uri (-> (config) :test-db :url)})

(def tables-spec
  {:manufacturers [[:id "int(11)" "not null" "auto_increment" "primary key"]
                   [:name "varchar(255)" "not null"]
                   [:country "varchar(63)" "not null"]
                   [:description :text]
                   [:useful "tinyint(1)" "not null"]]

   :widgets       [[:id "int(11)" "not null" "auto_increment" "primary key"]
                   [:name "varchar(255)" "not null" ]
                   [:type "varchar(63)" "not null"]
                   [:price_cents "int(11)" "not null"]
                   [:description "text"]
                   [:manufacturer_id "int(11)" "not null"]
                   [:created_at "datetime" "not null"]]})

(defn create-test-tables!
  "Create test tables"
  [conn]
  (jdbc/db-do-commands conn
                       (map
                        (fn [[table spec]]
                          (jdbc/create-table-ddl table
                                                 spec
                                                 {:table-spec "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;"}))
                        tables-spec)))

(defn drop-test-tables!
  "Drop test tables"
  [conn]
  (jdbc/db-do-commands conn
                       (map
                        (fn [[table _]]
                          (jdbc/drop-table-ddl (str "IF EXISTS " (name table))))
                        tables-spec)))

(defn reset-test-db!
  []
  (jdbc/with-db-connection [conn (db-spec)]
    (drop-test-tables! conn)
    (create-test-tables! conn)))

(defn sink-to-coll
  "Sink a manifold source into a collection. Return a core.async
  channel that will get a single value, the resulting collection, upon
  source being closed or after n elements consumed."
  ([source]
   (let [in (chan)
         _ (manifold.stream/connect source in)]
     (go-loop [res (transient [])]
       (let [val (<! in)]
         (if-not (nil? val)
           (recur (conj! res val))
           (persistent! res))))))
  ([source n]
   (let [in (chan)
         _ (manifold.stream/connect source in)]
     (go-loop [res (transient [])
               cnt n]
       (if (<= cnt 0)
         (persistent! res)
         (let [val (<! in)]
           (if (nil? val)
             (persistent! res)
             (recur (conj! res val)
                    (dec cnt)))))))))

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
  (str table "/" id))

(defn into-entity-map
  "Interpret the given ordered ops sequence by building a map of the
  final values for entities, identified by table and id. Inserts,
  updates and upserts add entities into map and deletes remove them."
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

