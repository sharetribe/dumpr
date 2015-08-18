(ns user
  (:require [dumpr.core :as dumpr]))

(def db-spec
  {:subprotocol "mysql"
   :subname "//127.0.0.1:3306/sharetribe_development?zeroDateTimeBehavior=convertToNull"
   :user "root"
   :password "not-root"})

(comment
  (def context (dumpr/create db-spec [:communities :listings :people :communities_listings]))
  (let [{:keys [events binlog-position]} (dumpr/load-tables context)]
    {:events events :binglog-position binlog-position})

  )
