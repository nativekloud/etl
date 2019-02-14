(ns etl.singer.state
  (:require [etl.singer.encoding :refer [encode decode]]
            [clojure.walk :refer [keywordize-keys]]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [try+ throw+]]))



(defn write-state [file state]
  (with-open [w (clojure.java.io/writer file)]
    (.write w (encode state))))

(defn read-state [file]
  (try+
   (keywordize-keys (decode (slurp file)))
   (catch Object _
     (log/error (:throwable &throw-context) "unexpected error"))))
