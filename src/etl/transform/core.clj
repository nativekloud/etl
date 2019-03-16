(ns etl.transform.core
  (:require [etl.singer.core :refer [transform]]
            [etl.singer.config :refer [load-config]]
            [etl.singer.messages :refer [write-schema write-record parse]]
            [etl.singer.encoding :refer [decode encode]]
            [clojure.tools.logging :as log]))


(defmethod transform "merge"
  [args]
  (let [config (load-config args)
        data (:data config)]
    (doseq [line (line-seq (java.io.BufferedReader. *in*))]
      (let [message (parse line)]
        (case (:type message)
          "RECORD"
          (write-record (:stream message) (merge (:record message) data) nil nil)

          "SCHEMA"
          (log/info "schema" message)

          "STATE"
          (log/info "STATE"  message)
          )))))


