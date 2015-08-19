(ns user
  (:require [dumpr.core :as dumpr]
            [clojure.core.async :as async :refer [<! go go-loop]]))

(def db-spec
  {:subprotocol "mysql"
   :subname "//127.0.0.1:3306/sharetribe_development?zeroDateTimeBehavior=convertToNull"
   :user "root"
   :password "not-root"})

(defn sink
  "Returns an atom containing a vector. Consumes values from channel
  ch and conj's them into the atom."
  [ch]
  (let [a (atom [])]
    (go-loop []
      (let [val (<! ch)]
        (when-not (nil? val)
          (swap! a conj val)
          (recur))))
    a))

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
  (take 3 (first @out-rows))
  (last @out-rows)
  (go (<! (:out res)))
  )
