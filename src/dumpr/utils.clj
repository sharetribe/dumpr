(ns dumpr.utils)

(defn infinite-retry
  ([f] (infinite-retry f identity))
  ([f handler] (infinite-retry f handler (constantly true)))
  ([f handler should-retry?] (infinite-retry f handler should-retry? (* 120 1000)))
  ([f handler should-retry? max-wait] (infinite-retry f handler should-retry? max-wait 1000))
  ([f handler should-retry? max-wait start-wait]
   (loop [wait start-wait]
     (let [wait (min wait max-wait)
           [success result] (try
                              [true (f)]
                              (catch Exception e
                                (handler e wait)
                                (Thread/sleep wait)
                                [false]))]
       (if success
         result
         (do
           (when (should-retry?)
             (recur (* wait 2)))))))))
