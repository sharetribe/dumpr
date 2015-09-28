(ns dumpr.utils)

(defn retry
  "Run the given function. If it throws, retry. Provide optional
  parameters to control the retry logic."
  ([f] (retry f identity))
  ([f handler] (retry f handler (constantly true)))
  ([f handler should-retry?] (retry f handler should-retry? (* 120 1000)))
  ([f handler should-retry? max-wait] (retry f handler should-retry? max-wait 1000))
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
