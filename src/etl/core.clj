(ns etl.core
  (:require [cli-matic.core :refer [run-cmd]]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.walk :as walk]
            [clojure.tools.logging :as log])
  (:gen-class))

;; helpers

(defn now []
  (.toString (java.time.LocalDateTime/now)))

(defn encode [data]
  (json/generate-string data))

(defn decode [data]
  (json/parse-string data))

(defn read-config-file [path]
  (walk/keywordize-keys (decode (slurp path))))

(defn load-config [args]
  (->(:config args)
     read-config-file))

(defn load-state [args]
  (->(:state args)
     read-config-file))

(defn load-catalog [args]
  (->(:catalog args)
     read-config-file))

(defn send-out [m]
  (println m))

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

;; public

(defn ->stream
  "create stream json"
  [tap_stream_id stream schema key-properties]
  {"tap_stream_id" tap_stream_id
   "stream"        stream
   "schema"        schema
   "key-properties" key-properties}
  )

(defn ->streams [streams]
  {"streams" streams})

;; TODO use multimethod
(defn write-message
  "Writes a message to *out*"
  [m]
  (send-out (encode
            (case (:type m)
              :record
              {"type"   "RECORD"
               "stream" (:stream m)
               ;; TODO extract as fn with RFC3339 formatting
               "time_extracted" (now)
               "record" (:record m)}

              :schema
              {"type"           "SCHEMA"
               "stream"         (:stream m)
               "schema"         (:schema m)
               "key_properties" (::key-properties m)}

              :state
              {"type"  "STATE"
               "value" (:value m)}))))

(defn write-record [stream record]
  (write-message {:type :record :stream stream :record record}))

(defn write-state [value]
  (write-message {:type :state, :value value}))

(defn write-schema [stream schema key-properties]
  (write-message {:type :schema :stream stream :schema schema :key-properties key-properties}))

;; Private helpers for parsig

;; TODO use mutlimethods ?

(defn parse [s]
  "Parses a message and returns it as a map"
  (let [m (decode s)]
    (when-not (map? m)
      (throw (Exception. "Message must be a map, got" s)))
    (case  (m "type")

      "RECORD"
      {:type :record
       :stream (m "stream")
       :record (m "record")}

      "SCHEMA"
      {:type :schema
       :stream (m "stream")
       :key-properties (m "msg-key-properties")
       :schema (m "schema")}

      "STATE"
      {:type :state
       :value (m "value")})))

;; DISCOVER command

(defmulti discover
  (fn [args] (:type args)))

(defmethod discover :default
  [args]
  (log/error "Discover not implemented for ->" args))

(defmethod discover "csv"
  [args]
  (let [config (load-config args)
        dirpath (:dirpath config)
        pattern (re-pattern (:pattern config))
        files (mapv #(.getPath %) (walk  dirpath pattern))]
    (-> (mapv #(->stream % % {} []) files) ;kinda ugly
        ->streams
        encode
        send-out)
    ))

;; TAP command

(defmulti tap
  (fn [args] (:type args)))

(defmethod tap :default
  [args]
  (log/info "tap type not implemented ->" args))

(defmethod tap "csv"
  [args]
  (let [config (load-config args)
        streams (:streams (load-catalog args))]
    (log/info "Starting import ...")
    (doseq [stream streams]
      (with-open [reader (io/reader (:stream stream))]
         (write-state {:value {}})
         (write-schema (:schema stream) (:schema stream) (:key-properties stream))
        (->> (csv/read-csv reader)
             csv-data->maps
             (mapv #(write-record (:stream stream) % ))
             ))
      (write-state {:value {}})
      )
    (log/info "Import finish successfully.")))

;; SINK command

(defmulti sink
  (fn [args] (:type args)))

(defmethod sink :default
  [args]
  (doseq [line (line-seq (java.io.BufferedReader. *in*))]
    (log/info (:type (parse line)))))

;; cli-matic config

(def CONFIGURATION
  {:app         {:command     "etl"
                 :description "ETL in Clojure"
                 :version     "0.1"}
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
