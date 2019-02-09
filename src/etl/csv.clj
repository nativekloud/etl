(ns etl.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [etl.singer.core :refer [tap discover sink load-config load-catalog]]
            [etl.singer.messages :refer [write-record write-schema write-state]]))

;; CSV

(defn walk
  "walk dirpath searching for pattern"
  [dirpath pattern]
  (doall (filter #(re-matches pattern (.getName %))
                 (file-seq (io/file dirpath)))))

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data) ;; First row is the header
            (map keyword) ;; Drop if you want string keys instead
            repeat)
       (rest csv-data)))

(defn write-csv
  "Takes a file (path, name and extension) and
   csv-data (vector of vectors with all values) and
   writes csv file."
  [file csv-data]
  (with-open [writer (io/writer file)]
    (csv/write-csv writer csv-data)))

(defn maps->csv-data
  "Takes a collection of maps and returns csv-data
   (vector of vectors with all values)."
  [maps]
  (let [columns (-> maps first keys)
        headers (mapv name columns)
        rows (mapv #(mapv % columns) maps)]
    (into [headers] rows)))

(defn write-csv-from-maps
  "Takes a file (path, name and extension) and a collection of maps
   transforms data (vector of vectors with all values)
   writes csv file."
  [file maps]
  (->> maps
       maps->csv-data
       (write-csv file)))

(defmethod tap "csv"
  [args]
  (let [config (load-config args)
        streams (:streams (load-catalog args))]
    (log/info "Starting import ...")
    (doseq [stream streams]
      (with-open [reader (io/reader (:stream stream))]
        (write-state {:value {}})
        (write-schema (:schema stream) (:schema stream) (:key-properties stream) (:bookmark-properties stream))
        (->> (csv/read-csv reader)
             csv-data->maps
             (mapv #(write-record (:stream stream) % nil nil))
             ))
      (write-state {:value {}})
      )
    (log/info "Import finish successfully.")))


;; (defn ->stream
;;   "create stream json"
;;   [tap_stream_id stream schema key-properties]
;;   {"tap_stream_id" tap_stream_id
;;    "stream"        stream
;;    "schema"        schema
;;    "key-properties" key-properties}
;;   )

;; (defn ->streams [streams]
;;   {"streams" streams})

;; (defmethod discover "csv"
;;   [args]
;;   (let [config (load-config args)
;;         dirpath (:dirpath config)
;;         pattern (re-pattern (:pattern config))
;;         files (mapv #(.getPath %) (walk  dirpath pattern))]
;;     (-> (mapv #(->stream % % {} []) files) ;kinda ugly
;;         ->streams
;;         encode
;;         send-out)
;;     ))
