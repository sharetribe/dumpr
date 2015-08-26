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

  (def stream-ctx (dumpr/binlog-stream context (:binlog-pos res)))
  (def out-events (sink-and-print (:out stream-ctx)))
  (dumpr/start-binlog-stream stream-ctx)
  (dumpr/close-binlog-stream stream-ctx)

  (count @out-events)
  (take 10 (drop 10 @out-events))

  )


