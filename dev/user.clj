(ns user
  (:require [reloaded.repl :refer [system init start stop go]]
            [system :as system]
            [dumpr.core :as dumpr]
            [dumpr.query :as query]
            [clojure.core.async :as async :refer [<!]]
            [taoensso.timbre :as log]))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (log/error ex "Uncaught exception on" (.getName thread)))))


;; (reloaded.repl/set-init! #(do (query/clear-cols-memo!) (system/only-stream {:file "Mikkos-MacBook-Pro-4-bin.000008", :position 163527})))
(reloaded.repl/set-init! #(do (query/clear-cols-memo!) (system/with-initial-load)))

(defn reset []
  (reloaded.repl/reset))


(comment
  (reset)
  (init)

  (-> system :loader :out-rows deref count)
  (-> system :loader :out-rows deref first)
  (-> system :loader :out-rows deref last)

  (-> system :streamer :out-events deref count)
  (-> system :streamer :out-events deref first)
  (-> system :streamer :out-events deref last)
  (-> system :streamer :binlog-pos)

  )


