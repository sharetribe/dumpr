(ns user
  (:require [dumpr.core :as dumpr]
            [clojure.core.async :as async :refer [<! go-loop]]))

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
  (def context (dumpr/create db-spec [:communities :listings :people :communities_listings]))
  (def res (dumpr/load-tables context))
  (def out-rows (sink (:out res)))
  (count @out-rows)
  (first @out-rows)
  (last @out-rows)
  )
