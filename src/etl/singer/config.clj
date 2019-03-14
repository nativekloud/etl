(ns etl.singer.config
  (:require [clojure.walk :as walk]
            [etl.singer.encoding :refer [decode encode]]
            [clojure.java.io :refer [writer]]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [etl.gcs.core :as gcs]))





(defmulti read-config-file (fn [path](keyword (first (s/split path #":")))))


(defmethod read-config-file :default [path]
  (walk/keywordize-keys (decode (slurp path)))
  )

(defmethod read-config-file :gcs [path]
  (let [[prefix url] (s/split path #"://")
        [bucket-name blob-name] (s/split url #"/")  ;FIXME: working only with single directory level
        ]
   ;[prefix url bucket-name blob-name]
    (walk/keywordize-keys (decode (String. (gcs/get-blob-content bucket-name blob-name ))))
    )
  )

(defmulti write-config-file (fn [path _](keyword (first (s/split path #":")))))

(defmethod write-config-file :default [path data]
  (with-open [w (writer path)]
    (.write w (encode data))))

(defmethod write-config-file :gcs [path data]
  (let [[prefix url] (s/split path #"://")
        [bucket-name blob-name] (s/split url #"/")]
    ;[bucket-name blob-name]
    (gcs/put-blob-string bucket-name blob-name "application/json" (encode  data))
    ))

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


(comment
  
  (read-config-file "gs://kazoup-test/test.txt")
  
  (s/split url #"/")  ;FIXME: working only with single directory level
  (= :gs (keyword (first (s/split "gs://test" #":"))))
  
  
  )
