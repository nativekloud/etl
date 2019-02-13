(ns etl.msgraph.groups
  (:require [etl.msgraph.client :refer [api-get settings]]))

;;; Groups

(defn get-groups []
  (api-get "/groups?$top=999"))

(defn set-groups! []
  (swap! settings assoc-in [:groups]  (get-groups)))
