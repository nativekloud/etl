(ns etl.singer.core
  (:require [etl.singer.encoding :refer [decode]]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]))

(defmulti tap
  (fn [args] (:type args)))

(defmethod tap :default
  [args]
  (log/info "tap type not implemented ->" args))

(defmulti sink
  (fn [args] (:type args)))

(defmethod sink :default
  [args]
  (log/error "Sink not implemented for ->" args))

(defmulti discover
  (fn [args] (:type args)))

(defmethod discover :default
  [args]
  (log/error "Discover not implemented for ->" args))

(defn now []
  (.toString (java.time.LocalDateTime/now)))

(defn read-config-file [path]
  (walk/keywordize-keys (decode (slurp path))))

(defn load-config
  "Reads config file and returns config map."
  [args]
  (->(:config args)
     read-config-file))

(defn load-state [args]
  (->(:state args)
     read-config-file))

(defn load-catalog [args]
  (->(:catalog args)
     read-config-file))
