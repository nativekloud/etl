(ns etl.singer.metadata)

;;; https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#metadata
(def default {:selected                   false                   
              :replication-method         "INCREMENTAL" 
              :replication-key           nil
              :view-key-properties       []
              :inclusion                 "automatic"
              :selected-by-default       false
              :valid-replication-keys    []
              :schema-name               ""
              :forced-replication-method "FULL_TABLE"
              :table-key-properties      []
              :is-view                   false
              :row-count                 nil
              :database-name             ""
              :sql-datatype              nil
              })

(defn ->MetadataEntry
  ([]
   default)
  ([stream-name]
   (-> default
       (assoc :schema-name stream-name)
       (assoc :database-name stream-name))))


(defn metadata [stream]
  (->MetadataEntry (:stream stream)))
