(ns etl.msgraph.core
  (:require [clojure.tools.logging :as log]
            [etl.singer.catalog :refer [stream write-streams]]
            [etl.singer.config :refer [read-state write-state load-catalog load-config]]
            [etl.singer.core :refer [discover tap]]
            [etl.singer.messages :refer [write-record]]
            [etl.msgraph.client :refer [set-params! dump-settings]]
            [etl.msgraph.users :refer [get-users-delta-callback get-users]]
            [etl.msgraph.emails :refer [get-user-folders messages-callback has-mail?]]
            [etl.msgraph.groups :refer [get-groups-delta-callback]]))


(defn users-left-to-scan [state users]
  (if-not (nil? (get-in state [:user :id])) 
    (drop-while #(not= (:id %) (get-in state [:user :id])) users)
    users))

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

(defn cb [results]
  (doseq [message results]
    ;(log/info message)
    (write-record (:stream stream) message nil nil)
    ) )

(defn msg->item [m]
  {:ID (:ID m)
   :endpoint-id ""
   :name (:subject m)
   :mime-type (get-in m [:body :content-type])
   :type ""
   :content (get-in m [:body :content])
   :modified-time (:LastModifiedTime m)
   :sharing ""
   :location "exchange"
   :category "email"
   :URL (:WebLink m)
   :sender (get-in m [:from :EmailAddress :address])
   :user (get-in m [:user :mail])
   :sharedWith ""})

;;;
(defmethod tap "msgraph"
  [args]
  (log/info "Starting msgraph tap.")
  ;; FIXME: start thread which will refresh token in atom when it's about to expire
  (set-params! (load-config args))
  ;;
  (let [initial-state (read-state  args)
        users-with-mail (has-mail? (get-users))
        users-to-scan (users-left-to-scan initial-state users-with-mail)]
    (doseq [user users-to-scan]
      (let [current-state  (read-state args)
            folders        (get-user-folders user)
            totalItemCount (reduce (fn [sum folder] (+ (:totalItemCount folder) sum))
                                   0
                                   folders)
            ]
        (when-not (zero? totalItemCount)
          (log/info "getting messages for" (:userPrincipalName user)
                    "totalItemCount:" totalItemCount
                    "folders:" (count folders))
          (write-state args (merge current-state {:user user}))                                ;
          (doseq [folder folders]
            (messages-callback user folder cb))
          ))))
  (log/info "msgraph tap finished sucesfully.")
  )



(comment

  (def args {:type "msgraph"
             :config "resources/tap-msgraph.json"
             :catalog "resources/tap-msgraph-catalog.json"})

  (discover args)
  (:id nil)
  (get-in nil [:user :id]) 
  (read-state "./test.txt")
  
  (def f (seq [{:totalItemCount 34} {:totalItemCount 12}]))

  (users-left-to-scan {:user {:id 2}} [{:id 1} {:id 2}])
  
  (reduce
   (fn [ right] (+ (:totalItemCount right) left) )
   0
   f)
  (def t 3)
  
  (when-not (zero? t)
   (println "test")
    (+ 1 2))
  
  
  
  )
