(ns dumpr.core-test
  (:require [clojure.test.check.generators :as gen]
            [clojure.test :as test :refer [deftest testing use-fixtures is]]
            [clojure.core.async :refer [<!!]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [com.gfredericks.test.chuck :as chuck]
            [dumpr.test-util :as test-util]
            [dumpr.core :as dumpr])
  (:import [java.util Date Calendar]))



;; Generators

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



;; Helpers

(defn- load-tables-to-coll [tables]
  (let [{:keys [conn-params]} (test-util/config)
        conf (dumpr/create-conf conn-params {})
        res (dumpr/load-tables tables conf)]
   (<!! (test-util/sink-to-coll (:out res)))))

(defn- annotate [annotation row]
  [annotation row])

(defn- table-id-key [[_ table id _ _]]
  (str table "-" id))



;; Tests

(deftest single-table-loading
  (checking "All ins2rted content is loaded" (chuck/times 3)
    [manufacturers (gen/no-shrink
                    (gen/vector
                     (gen/fmap (partial annotate :manufacturers) gen-manufacturer)
                     0 500))]
    (test-util/reset-test-db)
    (let [inserted (-> (test-util/insert-rows! manufacturers)
                       test-util/->table-to-rows
                       :manufacturers)
          loaded   (-> (load-tables-to-coll [:manufacturers])
                       test-util/->table-to-rows
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
    (test-util/reset-test-db)
    (let [inserted (test-util/insert-rows! (concat manufacturers widgets))
          loaded (load-tables-to-coll [:widgets :manufacturers])]
      (is (= (count inserted)
             (count loaded))
          "same number of rows")
      (is (= (sort-by table-id-key inserted)
             (sort-by table-id-key loaded))
          "same row contents"))))

