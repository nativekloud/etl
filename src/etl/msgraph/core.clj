(ns etl.msgraph.core
  (:require [clojure.tools.logging :as log]
            [etl.singer.catalog :refer [stream write-streams]]
            [etl.singer.state :refer [read-state write-state]]
            [etl.singer.core :refer [discover tap load-catalog load-config]]
            [etl.singer.messages :refer [write-record]]
            [etl.msgraph.client :refer [set-params! dump-settings]]
            [etl.msgraph.users :refer [get-users-delta]]))


(defn sync-do [stream config]
  (case (:stream stream)
    "users"
    (do
      (log/info (:stream stream) stream config)
      (set-params! config)
      (let [{:keys [results deltaLink]} (get-users-delta)]
        (doseq [user results] (write-record (:stream stream) user nil nil)))
     )
    "groups" 
    "messages"
    (log/info stream)))



;;; Singer


(defmethod discover "msgraph"
  [args]
  (let [config (load-config args)
        catalog (load-catalog args)
        users (stream "users" "users" {})
        groups (stream "groups" "groups" {})
        messages (stream "messages" "messages" {})
        streams [users groups messages]]
    (log/info "Writing simple catalog info.")    
    ;; Users stream 
    (write-streams streams )
    ))


(defmethod tap "msgraph"
  [args]
  (let [config (load-config args)
        catalog (load-catalog args)
        state (read-state (:state args))
        streams (:streams catalog)]
    ;; iterate streams and call API
    ;; pass (write-record resource->record )
    (log/info streams)
    (doseq [stream streams]
      (sync-do stream config))
    ))

