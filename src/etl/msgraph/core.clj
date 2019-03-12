(ns etl.msgraph.core
  (:require [clojure.tools.logging :as log]
            [etl.singer.catalog :refer [stream write-streams]]
            [etl.singer.state :refer [read-state write-state]]
            [etl.singer.core :refer [discover tap load-catalog load-config]]
            [etl.singer.messages :refer [write-record]]
            [etl.msgraph.client :refer [set-params! dump-settings]]
            [etl.msgraph.users :refer [get-users-delta-callback get-users]]
            [etl.msgraph.emails :refer [get-user-folders messages-callback has-mail?]]
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

;;; TODO: pickup a state if given
(defmethod tap "msgraph"
  [args]
  (log/info "Starting msgraph tap.")
  ;; FIXME: start thread which will refresh token in atom when it's about to expire
  (set-params! (load-config args))
  (doseq [user (has-mail? (get-users))]
    (let [folders        (get-user-folders user)
          totalItemCount (reduce (fn [sum folder] (+ (:totalItemCount folder) sum))
                                 0
                                 folders)
          current-state  (read-state (:state args))]
      (when-not (zero? totalItemCount)
        (log/info "getting messages for" (:userPrincipalName user)
                  "totalItemCount:" totalItemCount
                  "folders:" (count folders))
        (write-state (:state args) (merge {:user user} current-state))                                ;
        (doseq [folder folders]
          (messages-callback user folder
                             (fn [results] (doseq [message results]
                                             (write-record (:stream stream) message nil nil)))))
        )))
  (log/info "msgraph tap finished sucesfully.")
  )



(comment

  (def args {:type "msgraph"
             :config "resources/tap-msgraph.json"
             :catalog "resources/tap-msgraph-catalog.json"})

  (discover args)

  (def f (seq [{:totalItemCount 34} {:totalItemCount 12}]))

  (reduce
   (fn [ right] (+ (:totalItemCount right) left) )
   0
   f)
  (def t 3)
  
  (when-not (zero? t)
   (println "test")
    (+ 1 2))
  
  
  
  )
