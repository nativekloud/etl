(ns etl.csv.core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [etl.singer.core :refer [tap discover sink  now]]
            [etl.singer.config :refer [load-config load-catalog]]
            [etl.singer.messages :refer [write-record write-schema write-state parse]]
            [etl.singer.catalog :refer [stream write-streams]]))

;; CSV

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn walk
  "walk dirpath searching for pattern"
  [dirpath pattern]
  (log/info "Start walking filesystem to discover files." dirpath " " pattern )
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

(defn file->stream [path]
  (stream (uuid) path {}))

;;; Public

;; discover CSV files based on pattern and write streams to *out* for creating catalog
(defmethod discover "csv"
  [args]
  (let [config (load-config args)
        dirpath (:dirpath config)
        pattern (re-pattern (:pattern config))
        files (walk dirpath pattern)
        file-names (mapv #(.getPath %) files)
        streams (mapv #(file->stream %) file-names)]
    (write-streams streams)
     ))

(defmethod tap "csv"
  [args]
  (let [config (load-config args)
        streams (:streams (load-catalog args))]
    (log/info "Replicating data from CSV.")
    (doseq [stream streams]
      (with-open [reader (io/reader (:stream stream))]
        ;;(write-state {:value {}})
        (log/info "Replicating data from stream: " (:stream stream))
        (write-schema (:stream stream)
                      (:schema stream)
                      (:key-properties stream)
                      (:bookmark-properties stream))
        (->> (csv/read-csv reader)
             csv-data->maps
             (mapv #(write-record (:stream stream) % nil nil))
             ))
      (write-state {:start_date (now)})
      )
    (log/info "Tap exiting normally.")))

(defmethod sink "csv"
  [args]
  (let [config (load-config args)
        dirpath (:dirpath config)]
  (with-open [f (io/writer dirpath)]
   (doseq [line (line-seq (java.io.BufferedReader. *in*))]
     (let [message (parse line)]
         (case (:type message)
         
         "RECORD"
         (csv/write-csv f [(:record message)])

          "SCHEMA"
          (log/info "schema" message)

          "STATE"
          (log/info "STATE"  message)
         ))))))
