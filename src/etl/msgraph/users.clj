(ns etl.msgraph.users
  (:require [etl.msgraph.client :refer [api-get api-get-delta
                                        build-url settings]]
            [clojure.tools.logging :as log]))

;;; Users
;;; https://docs.microsoft.com/en-us/graph/api/resources/users?view=graph-rest-1.0

;;; https://graph.microsoft.com/{version}/{resource}?query-parameters

                                        ;
(def resource {:version "1.0"
               :resource "users"
               :query-params ""})

(defn get-users
  "Lists users in the organization."
  []
  (api-get "/users?$top=999"))


(defn get-users-delta 
  "Lists users in the organization and get deltaLink see axample at
  https://docs.microsoft.com/en-us/graph/delta-query-users
  returns map: {:results [...] :deltaLink \"url\"}
  "
  []
  (let [default (build-url "/users/delta?$top=999")
        url (:deltaLink (:users @settings) default )]
    (log/info url)
    (api-get-delta url)))


(defn set-users!
  "Set users in state atom"
  []
  (let [response (get-users-delta)]
    (if (nil? (:deltaLink (:users @settings)))
      (swap! settings assoc-in [:users] response)
      (log/info "TODO: handle update response:" response))))

(defn- get-user-info [user url]
  (let [url (str "/users/" (:id user) "/" url)]
    (log/info url)
    (api-get url)))


;;; not working ?
(defn get-user-by-id
  "Gets a specific user by id."
  [user]
  (api-get (str "/users/" (:id user))))

(defn get-user-photo
  "Gets the user's profile photo."
  [user]
  (get-user-info user "photo/$value"))

(defn get-user-messages
  "Lists the user's email messages in their primary inbox."
  [user]
  (get-user-info user "messages")
  )

(defn get-user-manager
  "Gets the user's manager."
  [user]
  (get-user-info user "manager")
  )

(defn get-user-events
  "Lists the user's upcoming events in their calendar."
  [user]
  
  (get-user-info user "events"))

(defn get-user-drive
  "Lists the user's upcoming events in their calendar."
  [user]
  (get-user-info user "drive"))

(defn get-user-memberOf
  "Lists the user's upcoming events in their calendar."
  [user]
  (get-user-info user "memberOf"))
