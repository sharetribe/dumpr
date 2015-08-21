(ns user
  (:require [dumpr.core :as dumpr]
            [clojure.core.async :as async :refer [<! go go-loop]]))

(def db-spec
  {;; :subname "//127.0.0.1:3306/sharetribe_development?zeroDateTimeBehavior=convertToNull"
   :user "root"
   :password "not-root"
   :host "127.0.0.1"
   :port 3306
   :db "sharetribe_development"
   :server-id 123})

(defn sink
  "Returns an atom containing a vector. Consumes values from channel
  ch and conj's them into the atom."
  ([ch] (sink ch identity))
  ([ch pr]
   (let [a (atom [])]
     (go-loop []
       (let [val (<! ch)]
         (if-not (nil? val)
           (do
             (pr val)
             (swap! a conj val)
             (recur))
           (pr "Channel closed."))))
     a)))

(defn sink-and-print [ch] (sink ch println))


(comment
  (def context
    (dumpr/create db-spec
                  [:communities
                   :listings
                   :people
                   {:table :communities_listings
                    :id-fn (fn [v]
                             (str (:community_id v) "-" (:listing_id v)))}]))

  (with-bindings {#'dumpr/*parallel-table-loads* 1}
    (dumpr/load-tables context (async/chan 10)))

  (def res (dumpr/load-tables context (async/chan 1000)))
  (def out-rows (sink (:out res)))
  (count @out-rows)
  (vec (take 2 (first @out-rows)))
  (last @out-rows)
  (go (println (<! (:out res))))

  (dumpr/query-binlog-position (:db-spec context))
  (def schema (dumpr/fetch-table-schema (:db-spec context) (-> context :conf :db) :listings))
  (dumpr/parse-table-schema schema :listings)

  (def stream-ctx (dumpr/stream-binlog context (:binlog-pos res)))
  (def out-events (sink-and-print (:out stream-ctx)))
  (count @out-events)

  (def table-map (atom {}))
  (map #(dumpr/handle-event % table-map (:conf context)) @out-events)

  (dumpr/handle-event (second @out-events) table-map (:conf context))

  (count @dumpr/events)
  (reset! dumpr/events [])

  (require '[dumpr.events :as events])
  (def test-events @dumpr/events)
  (map events/parse-event test-events)

  (.connect (:client stream-ctx) 1000)
  (.disconnect (:client stream-ctx))

  (last @dumpr/event)
  (map dumpr/parse-event @dumpr/events)
  (def ev (second @dumpr/events))
  (.. ev getHeader getNextPosition)
  (java.util.Date. (.. ev getHeader getTimestamp))
  (.getData ev)
  )


(comment

  (def group-ops
    (fn [rf]
      (let [prev (volatile! ::none)]
        (fn
          ([] (rf))
          ([result] (rf result))
          ([result input]
           (let [prior @prev]
             (vreset! prev input)
             (if (= input :table-map)
               result                     ; Delay table maps
               (if (= prior :table-map)
                 (rf result [prior input]) ; Return table-map op as pair
                 (rf result [input]))))))))) ; op without table map, just wrap

  (defn stop [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (if (= input :stop)
         (reduced result)
         (rf result input)))))

  (defn preserving-reduced
    [rf]
    #(let [ret (rf %1 %2)]
       (if (reduced? ret)
         (reduced ret)
         ret)))

  (def txs
    (fn [rf]
      (let [ongoing? (volatile! false)
            tx       (volatile! [])
            prf      (preserving-reduced rf)
            reset-tx (fn []
                       (let [tx-content @tx]
                         (vreset! ongoing? false)
                         (vreset! tx [])
                         tx-content))
            start-tx (fn [] (vreset! ongoing? true))]
        (fn
          ([] (rf))

          ([result] (rf result))

          ([result input]
           (condp = input
             :tx-start (do (start-tx)
                           result)

             :tx-commit (reduce prf result (reset-tx))

             :tx-rollback (do (reset-tx)
                              result)
             (if @ongoing?
               (do (vswap! tx conj input)
                   result)
               (rf result input))))))))


  (def coll [:op :tx-start :table-map :op :tx-commit :tx-start :table-map :op :tx-rollback :op])

  (transduce (comp txs (map #(pr (str "-" % "-"))) (take 2))
             conj
             []
             coll)

  )
