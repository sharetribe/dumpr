(ns user
  (:require [reloaded.repl :refer [system init start stop go]]
            [system :as system]
            [dumpr.core :as dumpr]
            [clojure.core.async :as async :refer [<!]]
            [taoensso.timbre :as log]))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (log/error ex "Uncaught exception on" (.getName thread)))))


(reloaded.repl/set-init! #(system/only-stream {:file nil :position 0}))
;; (reloaded.repl/set-init! #(system/with-initial-load))

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
  (-> system :streamer :out-events deref)
  (-> system :streamer :binlog-pos)
  )


