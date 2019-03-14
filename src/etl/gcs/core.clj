(ns etl.gcs.core
  (:require [clojure.tools.logging :as log])
  (:import (com.google.cloud.storage Storage
                                     StorageOptions
                                     BucketInfo
                                     Storage$BucketListOption
                                     Storage$SignUrlOption
                                     Storage$BlobGetOption
                                     Blob$BlobSourceOption
                                     BlobInfo
                                     BlobId
                                     Storage$BlobTargetOption
                                     
                                     )
           (java.util.concurrent TimeUnit)))


(defn to-clojure [bucket] {:name      (.getName bucket)
                           :location  (.getLocation bucket)
                           :self-link (.getSelfLink bucket)})

(defn list-buckets
  "paging doesn't work"
  []
  (loop [items nil
         op    (doto (-> (.getService (StorageOptions/getDefaultInstance)) (.list (into-array [(Storage$BucketListOption/pageSize 100)]))))]
    (let [results                   (-> (.getService (StorageOptions/getDefaultInstance)) (.list (into-array [(Storage$BucketListOption/pageSize 100)])))
          token     (.getNextPageToken results)
          new-items (.getValues results)]
                       (if (nil? token)
                         (lazy-cat items new-items)
                         (recur (lazy-cat items new-items) (doto (-> (.getService (StorageOptions/getDefaultInstance)) 
                                                                     (.list (into-array [(Storage$BucketListOption/pageSize 100)])) )
                                                             (.setPageToken token)) ))))
  
  )

(defn get-blob
  "gets  blob "
  [bucket-name blob-name]
  (let [service  (.getService (StorageOptions/getDefaultInstance))
        get-options (make-array Storage$BlobGetOption 0)
        source-options (make-array Blob$BlobSourceOption 0)
        blob (.get service bucket-name blob-name get-options)]
    blob))

(defn get-blob-content
  "gets content of the blob or returns nil if missing"
  [bucket-name blob-name]
  (let [service  (.getService (StorageOptions/getDefaultInstance)) 
        get-options (make-array Storage$BlobGetOption 0)
        source-options (make-array Blob$BlobSourceOption 0)
        blob (.get service bucket-name blob-name get-options)]
    (when blob (.getContent blob source-options))))

(defn put-blob-string
  [bucket-name blob-name content-type content]
  (let [service  (.getService (StorageOptions/getDefaultInstance))
        blob-info (.build (.setContentType (BlobInfo/newBuilder (BlobId/of bucket-name blob-name)) content-type))
        content-bytes (.getBytes content)
        target-options (make-array  Storage$BlobTargetOption 0)]
    (.create service blob-info content-bytes target-options)
    ))

;;; need to pass signing user
;;; https://googleapis.github.io/google-cloud-java/google-cloud-clients/apidocs/index.html?com/google/cloud/storage/package-summary.html

(defn get-signed-url
  "Generates a signed URL for a blob"
  [bucket-name blob-name duration]
  (let [blob (get-blob bucket-name blob-name)
        sign-option (make-array Storage$SignUrlOption 0)
        unit TimeUnit/MINUTES ]
    (.signUrl blob duration unit sign-option))
  )
