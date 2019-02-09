(ns etl.singer.messages
  (:require [etl.singer.encoding :refer [encode decode]]))

(defn ->RecordMessage
  "RECORD message.
    The RECORD message has these fields:
      * stream (string) - The name of the stream the record belongs to.
      * record (map) - The raw data for the record
      * version (optional, int) - For versioned streams, the version
        number. Note that this feature is experimental and most Taps and
        Targets should not need to use versioned streams.
      * time_extracted (optional, date)

  (->RecordMessage \"users\" {:id 1 :name \"Mary\"})
  "
  ([stream record]
   {:type "RECORD"
    :stream stream
    :record record})
  ([stream record time_extracted]
   {:type "RECORD"
      :stream stream
      :record record
      :time_extracted time_extracted})
  ([stream record time_extracted version]
   {:type "RECORD"
    :stream stream
    :record record
    :version version
    :time_extracted time_extracted})
  )

(defn ->SchemaMessage
  "SCHEMA message.
    The SCHEMA message has these fields:
      * stream (string) - The name of the stream this schema describes.
      * schema (dict) - The JSON schema.
      * key_properties (list of strings) - List of primary key properties."
  ([stream schema key-properties]
   {:type "SCHEMA"
    :stream stream
    :schema schema
    :key-properties key-properties})
  ([stream schema key-properties bookmark-properties]
   {:type "SCHEMA"
    :stream stream
    :schema schema
    :key-properties key-properties
    :bookmark-properties bookmark-properties}))

(defn ->StateMessage
  "STATE message.
    The STATE message has one field:
      * value (dict) - The value of the state."
  [value]
  {:type"STATE"
   :value value})

(defn parse [s]
  "Parse a message string into a Message map."
  (let [m (decode s)]
    (when-not (map? m)
      (throw (Exception. "Message must be a map, got" s)))
    (case  (m "type")

      "RECORD"
      (->RecordMessage (:stream m) (:record m) (:time_extracted m) (:version m))

      "SCHEMA"
      (->SchemaMessage (:stream m) (:schema m) (:key-properties m) (:bookmark-properties))

      "STATE"
      (->StateMessage (:stream m) (:value m)))))

(defn format-message [message]
  (encode message))

(defn write-message [message]
  (println (format-message message)))

(defn write-record [stream record time_extracted version]
  (write-message (->RecordMessage stream record time_extracted version)))

(defn write-records [stream records]
  (doseq [record records] (write-record stream record)))

(defn write-schema [stream schema key-properties bookmark-properties]
  (write-message (->SchemaMessage stream schema key-properties bookmark-properties)))

(defn write-state [value]
  (write-message (->StateMessage value)))
