(ns etl.core
  (:require [cli-matic.core :refer [run-cmd]]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.walk :as walk]
            [clojure.tools.logging :as log])
  (:gen-class))


;; helpers

(defn load-json [path]
  (walk/keywordize-keys (json/parse-string (slurp path))))



;; CSV
(defn walk [dirpath pattern]
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
  (->> maps maps->csv-data (write-csv file)))

;; public

(defn create-stream
  "create stream json"
  [tap_stream_id stream schema]
  {"tap_stream_id" tap_stream_id
   "stream"        stream
   "schema"        schema
   "key-properties" []}
  )


(defn write-message
  "Writes a message to *out*"
  [m]
  (println (json/generate-string
            (case (:type m)
              :record
              {"type"   "RECORD"
               "stream" (:stream m)
               ;; TODO extract as fn with RFC3339 formatting
               "time_extracted" (.toString (java.time.LocalDateTime/now))
               "record" (:record m)}

              :schema
              {"type"           "SCHEMA"
               "stream"         (:stream m)
               "schema"         (:schema m)
               "key_properties" (::key-properties m)}

              :state
              {"type"  "STATE"
               "value" (:value m)}))))

;; Private helpers for parsig


(defn parse [s]
  "Parses a message and returns it as a map"
  (let [m (json/parse-string s)]
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


(defn write-record [stream record]
  (write-message {:type :record :stream stream :record record}))

(defn write-state [value]
  (write-message {:type :state, :value value}))

(defn write-schema [stream schema key-properties]
  (write-message {:type :schema :stream stream :schema schema :key-properties key-properties}))


;; DISCOVER command

(defmulti discover
  (fn [args] (:type args)))

(defmethod discover :default
  [args]
  (log/info "Discover not implemented for ->" args))

(defmethod discover "csv"
  [args]
  (let [config (load-json (:config args))
        files (mapv #(.getPath %) (walk (:dirpath config) (re-pattern (:pattern config))))]
    (println (json/generate-string {:streams (mapv #(create-stream % % {}) files)} {:pretty true}))
    ))



;; TAP command

(defmulti tap
  (fn [{:keys [config state catalog type]}] type))

(defmethod tap :default
  [{:keys [config state catalog type]}]
  (println "tap type not implemented ->" type))

(defmethod tap "csv"
  [{:keys [config state catalog type]}]
  (let [config (load-json config)
        streams (:streams (load-json catalog))]
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
  (fn [{:keys [config type]}] type))

(defmethod sink :default
  [{:keys [config type]}]
  (doseq [line (line-seq (java.io.BufferedReader. *in*))]
    (println (:type (parse line)))))


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
