(ns user
  (:require [reloaded.repl :refer [system init start stop go]]
            [system :as system :refer [LibConf]]
            [dumpr.core :as dumpr]
            [clojure.core.async :as async :refer [<! go-loop >! timeout]]
            [taoensso.timbre :as log]
            [io.aviso.config :as config]
            [manifold.stream :as s]
            [manifold.deferred :as d]))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (log/error ex "Uncaught exception on" (.getName thread)))))

(defn config []
  (config/assemble-configuration {:prefix "dumpr"
                                  :profiles [:lib :dev]
                                  :schemas [LibConf]}))

(reloaded.repl/set-init! #(system/only-stream (config) {:file "Tamma-bin.000012" :position 0}))
;; (reloaded.repl/set-init! #(system/with-initial-load (config)))

(defn reset []
  (reloaded.repl/reset))

(defn delay-chan
  "Takes in and out channels and adds delay"
  [in out delay]
  (go-loop []
    (if-some [event (<! in)]
      (do
        (println (str "Got event, now timeout for " delay " millis"))
        (<! (timeout delay))
        (>! out event)
        (recur))
      (async/close!))))

(comment
  (reset)
  (init)

  (config)

  (-> system :loader :out-rows deref count)
  (-> system :loader :out-rows deref first)
  (-> system :loader :out-rows deref last)

  (-> system :streamer :out-events deref count)
  (-> system :streamer :out-events deref first)
  (-> system :streamer :out-events deref last)
  (-> system :streamer :out-events deref)
  (-> system :streamer :stream)

  (-> system :conf)

  (reloaded.repl/stop)

  (dumpr/valid-binlog-pos? (:conf system) {:file "Tamma-bin.000013" :position 0})
  )


