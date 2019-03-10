(ns etl.msgraph.core
  (:require [clojure.tools.logging :as log]
            [etl.singer.catalog :refer [stream write-streams]]
            [etl.singer.state :refer [read-state write-state]]
            [etl.singer.core :refer [discover tap load-catalog load-config]]
            [etl.singer.messages :refer [write-record]]
            [etl.msgraph.client :refer [set-params! dump-settings]]
            [etl.msgraph.users :refer [get-users-delta-callback]]
            [etl.msgraph.groups :refer [get-groups-delta-callback]]))


(defn sync-do [stream config state]
  (case (:stream stream)
    "users"
    (do
      (log/info "Syncing users.")
      (set-params! config)
     
      ;
      (let [results  (get-users-delta-callback (fn [results] (doseq [user results]
                                                               (write-record (:stream stream) user nil nil))))]
        (write-state  state (merge {:users results} (read-state state)))                                ;
        )
      )
    "groups"
     (do
      (log/info "Syncing groups.")
      (set-params! config)
     
      ;
      (let [results  (get-groups-delta-callback (fn [results] (doseq [user results]
                                                                (write-record (:stream stream) user nil nil))))]
        (write-state state (merge {:groups results} (read-state state)))                                ;
        )
      )
    "emails"
    (log/info stream)))



;;; Singer


(defmethod discover "msgraph"
  [args]
  (let [config (load-config args)
        ;catalog (load-catalog args)
        users (stream "users" "users" {})
        groups (stream "groups" "groups" {})
        emails (stream "emails" "emails" {})
        streams [users groups emails]]
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
    (log/info "Starting msgraph tap.")
    (doseq [stream streams]
      (sync-do stream config (:state args)))
    ))


(comment

  (def args {:type "msgraph"
             :config "resources/tap-msgraph.json"
             :catalog "resources/tap-msgraph-catalog.json"})

  (discover args)
  
  )
