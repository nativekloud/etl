(ns etl.core
  (:require [say-cheez.core :refer [capture-build-env-to]]
            [cli-matic.core :refer [run-cmd]]
            [etl.singer.core :refer [tap sink discover]]
            [samsara.trackit :refer [start-reporting!]]
            ;; plugins
            [etl.csv.core]
            [etl.pubsub.core]
            [etl.msgraph.core])
  (:import [java.util.concurrent TimeUnit])
  (:gen-class))

;;(set! *warn-on-reflection* false)

;;
(capture-build-env-to BUILD)
;; cli-matic config


;; (start-reporting!
;;    {:type                        :console
;;     ;; how often the stats will be displayed
;;     :reporting-frequency-seconds 300
;;     ;; which output stream should be used stdout or stderr
;;     :stream                      (System/err)
;;     ;; unit to use to display rates
;;     :rate-unit                   TimeUnit/SECONDS
;;     ;; unit to use to display durations
;;     :duration-unit               TimeUnit/MILLISECONDS
;;     ;; to disable metrics instrumentation
;;     :jvm-metrics :none})


(def CONFIGURATION
  {:app         {:command     "etl"
                 :description "ETL in Clojure"
                 :version     (:version (:project BUILD))
                 :build-at (:build-at (:project BUILD))}
   :global-opts [{:option  "config"
                  :as      "config file url or path"
                  :type    :string
                  :default "config.json"}
                 {:option  "state"
                  :as      "a file or URL for state"
                  :type    :string
                  :default "state.json"}
                 {:option  "catalog"
                  :as      "catalog of avaiable streams"
                  :type    :string
                  :default "catalog.json"}
                 ]
   :commands    [{:command     "tap" :short "source"
                  :description ["Reads from tap and prints to *out*"]
                  :opts        [{:option  "type"
                                 :as      "Type of tap [csv]"
                                 :type    :string
                                 :default "csv"}]
                  :runs        tap}
                 {:command     "sink" :short "target"
                  :description ["read *stdin* and writes to target"]
                  :opts        [{:option  "type"
                                 :as      "Type of tap [csv]"
                                 :type    :string
                                 :default "csv"}]
                  :runs        sink}
                 {:command     "discover" :short "sniff"
                  :description ["read tap config and and write streams nad schema into catalog"]
                  :opts        [{:option  "type"
                                 :as      "Type of tap [csv]"
                                 :type    :string
                                 :default "csv"}]
                  :runs        discover}]})

(defn -main [& args]
  (run-cmd args CONFIGURATION))
