(ns dumpr.core-test
  (:require [clojure.test.check.generators :as gen]
            [clojure.test :as test :refer [deftest testing use-fixtures is]]
            [clojure.core.async :refer [<!!]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [com.gfredericks.test.chuck :as chuck]
            [com.gfredericks.test.chuck.generators :as gen']
            [dumpr.test-util :as test-util]
            [dumpr.core :as dumpr])
  (:import [java.util Date Calendar]))



;; Generators

(defn gen-manufacturer
  ([]
   (gen/fmap
    #(zipmap [:name :country :description] %)
    (gen/tuple (gen/not-empty gen/string-alphanumeric)
               (gen/elements ["us" "fi" "fr" "de" "uk" "nz" "au"])
               gen/string-alphanumeric)))
  ([ids]
   (gen/fmap
    #(zipmap [:id :name :country :description] %)
    (gen/tuple (gen/elements ids)
               (gen/not-empty gen/string-alphanumeric)
               (gen/elements ["us" "fi" "fr" "de" "uk" "nz" "au"])
               gen/string-alphanumeric))))

(defn- clear-millis [date]
  (let [c (doto (Calendar/getInstance)
            (.setTime date)
            (.clear Calendar/MILLISECOND))]
    (.getTime c)))

(defn gen-widget
  ([]
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
  ([mfids wids]
   (gen/fmap
    #(zipmap
      [:id :name :type :price_cents :description :manufacturer_id :created_at]
      %)
    (gen/tuple (gen/elements wids)
               (gen/not-empty gen/string-alphanumeric)
               (gen/elements ["hobla" "bobla" "nano"])
               gen/pos-int
               gen/string-alphanumeric
               (gen/elements mfids)
               (gen/fmap (fn [^long time] (clear-millis (Date. time)))
                         (gen/choose 1232391819815 1442391819815))))))



(def gen-ids
  (gen/fmap (comp vec set)
            (gen/not-empty (gen/vector gen/s-pos-int))))


(defn gen-random-ops [mfids wids]
  (letfn [(upsert [table content]
            [:upsert table (:id content) content nil])
          (gen-delete [table ids]
            (gen/fmap (fn [id] [:delete table id nil nil])
                      (gen/elements ids)))]
    (gen/vector (gen/frequency [[6 (gen/fmap (partial upsert :manufacturers)
                                             (gen-manufacturer mfids))]
                                [6 (gen/fmap (partial upsert :widgets)
                                             (gen-widget mfids wids))]
                                [2 (gen-delete :manufacturers mfids)]
                                [2 (gen-delete :widgets wids)]]))))

(defn normalize-ops
  "Given a generated sequence of upsert and delete ops process the
  sequence into something that could actually happen by:

  1. Turn first upserts into inserts and following upserts on same id
  into updates.
  2. Remove deletes of non-existing rows"
  [ops]
  (-> (reduce (fn [{:keys [result entities]}
                   [type _ _ _ _ :as op]]
                (let [key (test-util/table-id-key op)]
                  (cond
                    (= type :upsert) (if (entities key)
                                       {:result (conj result
                                                      (assoc op 0 :update))
                                        :entities entities}
                                       {:result (conj result
                                                      (assoc op 0 :insert))
                                        :entities (assoc entities key true)})
                    (= type :delete) (if (entities key)
                                       {:result (conj result op)
                                        :entities (dissoc entities key)}
                                       {:result result
                                        :entities entities}))))
              {:result []
               :entities {}}
              ops)
      :result))


(def gen-ops-sequence
  (gen/fmap normalize-ops
            (gen/bind (gen/vector gen-ids 2)
                      #(apply gen-random-ops %))))


(defn partition-2
  "Partition the collection returned by coll-gen randomly into two."
  [coll-gen]
  (gen'/for [coll coll-gen
             ppoint (gen/choose 0 (Math/max 0 (dec (count coll))))]
    (split-at ppoint coll)))


;; Helpers

(defn- load-tables-to-coll [tables]
  (let [{:keys [conn-params]} (test-util/config)
        conf (dumpr/create-conf conn-params {})
        res (dumpr/load-tables tables conf)]
    {:out (<!! (test-util/sink-to-coll (:out res)))
     :binlog-pos (:binlog-pos res)}))

(defn- create-and-start-stream [binlog-pos]
  (let [{:keys [conn-params]} (test-util/config)
        conf (dumpr/create-conf conn-params {})
        stream (dumpr/binlog-stream conf binlog-pos [:widgets :manufacturers])]
    (dumpr/start-binlog-stream stream)
    stream))

(defn- stream-to-coll-and-close [stream n]
  (let [out (<!! (test-util/sink-to-coll (:out stream) n))]
    (dumpr/close-binlog-stream stream)
    out))


;; Tests

(deftest table-loading
  (checking "All inserted content is loaded" (chuck/times 10)
    [ops gen-ops-sequence]
    (test-util/reset-test-db!)
    (test-util/into-test-db! ops)
    (let [expected (test-util/into-entity-map ops)
          actual (-> (load-tables-to-coll #{:widgets :manufacturers})
                     :out
                     test-util/into-entity-map)]
      (is (= expected actual)))))

(deftest streaming
  (checking "Streaming" (chuck/times 10)
    [[initial streamed] (partition-2 gen-ops-sequence)]
    (test-util/reset-test-db!)
    (test-util/into-test-db! initial)
    (let [{:keys [out binlog-pos]} (load-tables-to-coll [:widgets :manufacturers])
          stream                   (create-and-start-stream binlog-pos)
          _                        (test-util/into-test-db! streamed)
          stream-out               (stream-to-coll-and-close stream (count streamed))]
      (is (= (test-util/into-entity-map (concat initial streamed))
             (test-util/into-entity-map (concat out stream-out)))))))


(comment
  (test/run-tests)
  (let [ops (last (gen/sample gen-ops-sequence 25))]
    (test-util/into-entity-map ops))

  (let [ops (last (gen/sample gen-ops-sequence 25))]
    (test-util/reset-test-db!)
    (test-util/into-test-db! ops)
    (test-util/into-entity-map ops))

  (last (gen/sample (partition-2 gen-ops-sequence) 5))
  )
