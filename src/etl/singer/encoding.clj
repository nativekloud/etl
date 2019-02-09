(ns etl.singer.encoding
  (:require [cheshire.core :as json]))

(defn encode [data]
  (json/generate-string data))

(defn decode [data]
  (json/parse-string data))
