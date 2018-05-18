(ns dumpr.core-test
  (:require [clojure.test.check.generators :as gen]
            [clojure.test :as test :refer [deftest testing use-fixtures is]]
            [clojure.core.async :refer [<!!] :as async]
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
    #(zipmap [:name :country :description :useful] %)
    (gen/tuple (gen/not-empty gen/string-alphanumeric)
               (gen/elements ["us" "fi" "fr" "de" "uk" "nz" "au"])
               gen/string-alphanumeric
               (gen/elements [0 1]))))
  ([ids]
   (gen/fmap
    #(zipmap [:id :name :country :description :useful] %)
    (gen/tuple (gen/elements ids)
               (gen/not-empty gen/string-alphanumeric)
               (gen/elements ["us" "fi" "fr" "de" "uk" "nz" "au"])
               gen/string-alphanumeric
               (gen/elements [0 1])))))

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
                                        :entities (conj entities key)})
                    (= type :delete) (if (entities key)
                                       {:result (conj result op)
                                        :entities (disj entities key)}
                                       {:result result
                                        :entities entities}))))
              {:result []
               :entities #{}}
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
        stream (dumpr/create-table-stream conf tables)
        _ (dumpr/start-stream! stream)]
    {:out (<!! (test-util/sink-to-coll (dumpr/source stream)))
     :binlog-pos (dumpr/next-position stream)}))

(defn- create-and-start-stream [binlog-pos tables]
  (let [{:keys [conn-params]} (test-util/config)
        conf (dumpr/create-conf conn-params {})
        stream (if (seq tables)
                 (dumpr/create-binlog-stream conf binlog-pos #{:widgets :manufacturers})
                 (dumpr/create-binlog-stream conf binlog-pos))]
    (dumpr/start-stream! stream)
    stream))

(defn- stream-to-coll-and-close
  ([stream n] (stream-to-coll-and-close stream n 60000))
  ([stream n timeout-ms]
   (let [timeout (async/timeout timeout-ms)
         [out p] (async/alts!! [timeout
                                (test-util/sink-to-coll (dumpr/source stream) n)])]
     (dumpr/stop-stream! stream)
     (if (= p timeout)
       (throw (ex-info "Timeout waiting for ops in stream" {}))
       out))))


;; Tests

(deftest table-loading
  (checking "All content inserted before table load is returned in it" (chuck/times 10)
    [ops gen-ops-sequence]

    (test-util/reset-test-db!)
    (test-util/into-test-db! ops)
    (let [expected (test-util/into-entity-map ops)
          actual (-> (load-tables-to-coll [:widgets :manufacturers])
                     :out
                     test-util/into-entity-map)]
      (is (= expected actual)))))

(deftest streaming
  (checking "All content inserted after table load is returned in stream" (chuck/times 15)
    [tables             (gen/elements [#{:widgets :manufacturers} nil #{}])
     [initial streamed] (partition-2 gen-ops-sequence)]

    (test-util/reset-test-db!)
    (test-util/into-test-db! initial)
    (let [{:keys [out binlog-pos]} (load-tables-to-coll [:widgets :manufacturers])
          stream                   (create-and-start-stream binlog-pos tables)
          _                        (test-util/into-test-db! streamed)
          stream-out               (stream-to-coll-and-close stream (count streamed))]
      (is (= (test-util/into-entity-map (concat initial streamed))
             (test-util/into-entity-map (concat out stream-out)))))))

;; Should be high enough to cause MySQL to split ops in binlog
(def test-data-count 300)

(def test-data
  (into []
        (mapcat
         (fn [i]
           [[:insert :manufacturers i {:id i
                                       :name (str "man" i)
                                       :country "fi"
                                       :description (str "Description of man" i)
                                       :useful 1}
             nil]
            [:insert :widgets i {:id i
                                 :name (str "widget" i)
                                 :type "a-type"
                                 :price_cents 10
                                 :description (str "description of widget" i)
                                 :manufacturer_id (inc (mod i 10))
                                 :created_at (java.util.Date.)}
             nil]]))
        (range 1 (inc test-data-count))))

(deftest streaming-multirow-updates
  (testing "updates to multiple rows in same query"
    (test-util/reset-test-db!)
    (test-util/into-test-db! test-data)
    (let [{:keys [out binlog-pos]} (load-tables-to-coll [:widgets :manufacturers])
          stream (create-and-start-stream binlog-pos [:widgets :manufacturers])
          _ (test-util/update-in-db! :manufacturers {:description "updated description"} ["id <= ?" test-data-count])
          _ (test-util/delete-from-db! :widgets ["id <= ?" test-data-count])
          stream-out (stream-to-coll-and-close stream (* 2 test-data-count) 10000)
          result-entities (test-util/into-entity-map (concat test-data stream-out))]
      ;; check all descriptions match and no widgets remain
      (is (= test-data-count (-> result-entities vals count)))
      (is (every? #(= (get % :description)
                      "updated description")
                  (vals result-entities))))))

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
