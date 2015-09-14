(ns dumpr.core-test
  (:require [clojure.test.check.generators :as gen]
            [clojure.test :as test :refer [deftest testing use-fixtures is]]
            [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [<! >! <!! go-loop chan]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [joplin.repl :as repl]
            [schema.core :as s]
            [io.aviso.config :as config]
            [system :refer [LibConf]]
            [dumpr.core :as dumpr]))

(defn- config []
  (config/assemble-configuration {:prefix "dumpr"
                                  :profiles [:lib :test]
                                  :schemas [LibConf]}))

(defn- joplin-config []
  (:joplin (config)))

(defn- db-spec []
  {:connection-uri (-> (joplin-config) :databases :sql-test :url)})

;; (s/defschema Widget
;;   "Widget"
;;   {:id s/Int
;;    :name s/Str
;;    :type (s/enum [:hobla :bobla :nano])
;;    :price_cents (s/both s/Int (s/pred pos? 'pos?'))
;;    :description s/Str
;;    :manufacturer_id s/Int
;;    :created_at s/Inst})

;; (s/defschema Manufacturer
;;   "Manufacturer"
;;   {:id s/Int
;;    :name s/Str
;;    :country (s/enum [:us :fi :fr :de :uk :nz :au])
;;    :description s/Str})


(def gen-manufacturer
  (gen/fmap
   #(zipmap [:name :country :description] %)
   (gen/tuple (gen/not-empty gen/string-alphanumeric)
                    (gen/elements ["us" "fi" "fr" "de" "uk" "nz" "au"])
                    gen/string-alphanumeric)))

(defn insert-manufacturers! [manufacturers]
  (jdbc/with-db-connection [conn (db-spec)]
    (reduce #(conj %1
                   (assoc %2
                          :id
                          (-> (jdbc/insert! conn :manufacturers %2)
                              first
                              :generated_key)))
            []
            manufacturers)))

(defn setup-and-clean-db [f]
  (repl/reset (joplin-config) :test :sql-test)
  (f)
  (repl/rollback (joplin-config) :test :sql-test 1))

(use-fixtures :each #'setup-and-clean-db)


(defn- sink-to-coll [in]
  (let [out (chan)]
    (go-loop [res (transient [])]
      (let [val (<! in)]
        (if-not (nil? val)
          (recur (conj! res val))
          (do
            (pr "Channel closed.")
            (>! out (persistent! res))))))
    out))

(defn- load-tables-to-coll [tables]
  (let [{:keys [conn-params]} (config)
        conf (dumpr/create-conf conn-params {})
        res (dumpr/load-tables tables conf)]
   (<!! (sink-to-coll (:out res)))))

(defn ->table-to-rows [upserts]
  (into {}
        (->> (group-by second upserts)
             (map (fn [[table rows]]
                    [table (map #(nth % 3) rows)])))))

(deftest single-table-loading
  (checking "All inserted content is loaded" 10
    [manufacturers (gen/no-shrink (gen/vector gen-manufacturer 10 100))]
    (repl/reset (joplin-config) :test :sql-test)
    (let [inserted (insert-manufacturers! manufacturers)
          loaded (-> (load-tables-to-coll [:manufacturers])
                     ->table-to-rows
                     :manufacturers)]
      (is (= (count inserted)
             (count loaded))
          "same number of rows")
      (is (= (sort-by :id inserted)
             (sort-by :id loaded))
          "same row contents"))))


(comment
  (test/run-tests)
  )
