(ns etl.singer.catalog
  (:require [etl.singer.encoding :refer [encode]]))


(defn ->CatalogEntry
  ([tap_stream_id stream]
   {:tap_stream_id tap_stream_id
    :stream stream})
  ([tap_stream_id stream key-properties schema
     replication-key is-view database table
    row-count stream-alias metadata replication-method]
   
   {:tap_stream_id      tap_stream_id
    :stream             stream
    :key-properties     key-properties
    :schema             schema
    :replication-key    replication-key
    :is-view            is-view
    :database           database
    :table              table
    :row-count          row-count
    :stream-alias       stream-alias
    :metadata           metadata
    :replication-method replication-method}))

(defn ->Catalog
  [streams]
  {:streams streams})


(defn stream [tap_stream_id stream]
  (->CatalogEntry tap_stream_id stream)
  )

(defn write-streams [streams]
  (println (encode  (->Catalog streams))))
