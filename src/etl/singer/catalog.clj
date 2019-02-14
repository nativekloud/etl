(ns etl.singer.catalog
  (:require [etl.singer.encoding :refer [encode]]))


(defn ->CatalogEntry
  ([tap_stream_id stream schema]
   {:tap_stream_id tap_stream_id
    :stream stream
    :schema schema}))

(defn ->Catalog
  [streams]
  {:streams streams})

(defn format-stream [stream]
  (encode stream))

(defn stream [tap_stream_id stream schema]
  (->CatalogEntry tap_stream_id stream schema)
  )

(defn write-streams [streams]
  (println (format-stream (->Catalog streams))))
