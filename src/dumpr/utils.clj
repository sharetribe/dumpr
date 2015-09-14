(ns dumpr.utils)

(defn infinite-retry
  ([f] (infinite-retry f identity))
  ([f handler] (infinite-retry f handler (* 120 1000)))
  ([f handler max-wait] (infinite-retry f handler max-wait 1000))
  ([f handler max-wait start-wait]
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
         (recur (* wait 2)))))))
