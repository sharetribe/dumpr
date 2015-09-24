(ns dumpr.core-test
  (:require [clojure.test.check.generators :as gen]
            [clojure.test :as test :refer [deftest testing use-fixtures is]]
            [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer [<! >! <!! go-loop chan]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [com.gfredericks.test.chuck :as chuck]
            [joplin.repl :as repl]
            [schema.core :as s]
            [io.aviso.config :as config]
            [system :refer [LibConf]]
            [dumpr.core :as dumpr])
  (:import [java.util Date Calendar]))

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

(defn- clear-millis [date]
  (let [c (doto (Calendar/getInstance)
            (.setTime date)
            (.clear Calendar/MILLISECOND))]
    (.getTime c)))

(def gen-widgets
  (gen/fmap
   #(zipmap
     [:name :type :price_cents :description :manufacturer_id :created_at]
     %)
   (gen/tuple (gen/not-empty gen/string-alphanumeric)
              (gen/elements ["hobla" "bobla" "nano"])
              gen/pos-int
              gen/string-alphanumeric
              gen/pos-int
              (gen/fmap (fn [^long time] (clear-millis (Date. time)))
                        (gen/choose 1232391819815 1442391819815)))))


(defn insert-rows! [rows]
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


(defn setup-and-clean-db [f]
  (repl/reset (joplin-config) :test :sql-test)
  (f)
  (repl/rollback (joplin-config) :test :sql-test 1))

;; (use-fixtures :each #'setup-and-clean-db)


(defn- sink-to-coll [in]
  (go-loop [res (transient [])]
    (let [val (<! in)]
      (if-not (nil? val)
        (recur (conj! res val))
        (persistent! res)))))

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

(defn- annotate [annotation row]
  [annotation row])

(defn- table-id-key [[_ table id _ _]]
  (str table "-" id))

(deftest single-table-loading
  (checking "All ins2rted content is loaded" (chuck/times 3)
    [manufacturers (gen/no-shrink
                    (gen/vector
                     (gen/fmap (partial annotate :manufacturers) gen-manufacturer)
                     0 500))]
    (repl/reset (joplin-config) :test :sql-test)
    (let [inserted (-> (insert-rows! manufacturers)
                       ->table-to-rows
                       :manufacturers)
          loaded   (-> (load-tables-to-coll [:manufacturers])
                       ->table-to-rows
                       :manufacturers)]
      (is (= (count inserted)
             (count loaded))
          "same number of rows")
      (is (= (sort-by :id inserted)
             (sort-by :id loaded))
            "same row contents"))))

(deftest multi-table-loading
  (checking "All inserted content is loaded from all tables" (chuck/times 3)
    [manufacturers (gen/no-shrink
                    (gen/vector
                     (gen/fmap (partial annotate :manufacturers) gen-manufacturer)
                     0 100))
     widgets (gen/no-shrink
              (gen/vector
               (gen/fmap (partial annotate :widgets) gen-widgets)
               0 500))]
    (repl/reset (joplin-config) :test :sql-test)
    (let [inserted (insert-rows! (concat manufacturers widgets))
          loaded (load-tables-to-coll [:widgets :manufacturers])]
      (is (= (count inserted)
             (count loaded))
          "same number of rows")
      (is (= (sort-by table-id-key inserted)
             (sort-by table-id-key loaded))
          "same row contents"))))


(comment
  (def mgen (gen/no-shrink
                    (gen/vector
                     (gen/fmap (fn [m] [:manufacturer m]) gen-manufacturer)
                     0 100)))
  (insert-rows! (gen/sample mgen 10))
  (test/run-tests)
  (gen/sample (gen-widgets [1 2 3 4 5]) 10)
  (gen/sample (gen/fmap (comp vec set) (gen/vector gen/pos-int)))
  )
