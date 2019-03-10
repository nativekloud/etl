(ns etl.msgraph.groups
  (:require [etl.msgraph.client :refer [api-get settings
                                        api-get-delta
                                        api-get-delta-callback
                                        build-url]]
            [clojure.tools.logging :as log]))

;;; Groups

(defn get-groups []
  (api-get "/groups?$top=999"))

(defn set-groups! []
  (swap! settings assoc-in [:groups]  (get-groups)))

(defn get-groups-delta 
  "Lists users in the organization and get deltaLink see axample at
  https://docs.microsoft.com/en-us/graph/delta-query-users
  returns map: {:results [...] :deltaLink \"url\"}
  "
  []
  (let [default (build-url "/groups/delta?$top=999")
        url (:deltaLink (:users @settings) default )]
    (log/info url)
    (api-get-delta url)))

(defn get-groups-delta-callback
  "Lists users in the organization and get deltaLink see axample at
  https://docs.microsoft.com/en-us/graph/delta-query-users
  returns map: {:results [...] :deltaLink \"url\"}
  "
  [fn]
  (let [default (build-url "/groups/delta?$top=999")
        url (:deltaLink (:users @settings) default )]
    (log/info url)
    (api-get-delta-callback url fn)))
