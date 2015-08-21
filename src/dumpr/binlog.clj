(ns dumpr.binlog
  (:require [clojure.core.async :as async :refer [chan >!!]]
            [taoensso.timbre :as log]
            [dumpr.query :as query])
  (:import [com.github.shyiko.mysql.binlog
            BinaryLogClient
            BinaryLogClient$EventListener
            BinaryLogClient$LifecycleListener]
           [com.github.shyiko.mysql.binlog.event EventType]) )

