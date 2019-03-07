(ns etl.singer.config
  (:require [clojure.walk :as walk]
            [etl.singer.encoding :refer [decode encode]]
            [clojure.java.io :refer [writer]]))

(defn read-config-file [path]
  (walk/keywordize-keys (decode (slurp path))))

(defn write-config-file [path data]
  (with-open [w (writer path)]
    (.write w (encode data))))

(defn load-config
  "Reads config file and returns config map."
  [args]
  (->(:config args)
     read-config-file))

(defn write-config
  [args data]
  (write-config-file (:config args) data))

(defn load-state [args]
  (->(:state args)
     read-config-file))

(defn write-state [args data]
  (write-config-file (:state args) data))

(defn load-catalog [args]
  (->(:catalog args)
     read-config-file))

(defn write-catalog [args data]
  (write-config-file (:catalog args) data))
