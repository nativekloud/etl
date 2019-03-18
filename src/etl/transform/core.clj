(ns etl.transform.core
  (:require [etl.singer.core :refer [transform]]
            [etl.singer.config :refer [load-config]]
            [etl.singer.messages :refer [write-schema write-record parse]]
            [etl.singer.encoding :refer [decode encode]]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [clojure.walk :as walk]))


(defn get-sender [m]
  (get-in m [:sender :emailAddress :address]))

(defn get-user-mail [m]
  (get-in m [:user :mail]))

(defn shared-with [m]
  (let [cc         (:ccRecipients m)
        to         (:toRecipients m)
        bcc        (:bccRecipients m)
        recipients (concat cc to bcc)]
      (mapv #(get-in % [:emailAddress :address]) recipients)
      ))
  
(def transformation {:id            [identity :id]
                     :name          [identity :subject]
                     :modified-time [identity :lastModifiedDateTime]
                     :mime-type     [:contentType :body]
                     :content       [:content :body]
                     :url           [identity :webLink]
                     :sender        [get-sender identity]
                     :user          [get-user-mail identity]
                     :shared-with   [shared-with identity]})
  

  
;;   The transformation function
(defn transform-map [m fm]
  (into {} (map (fn [[k v]]
                  [k (apply (first v)
                            ((apply juxt (rest v)) m))])
                fm)))



(defmethod transform "merge"
  [args]
  (let [config (load-config args)
        data (:data config)]
    (doseq [line (line-seq (java.io.BufferedReader. *in*))]
      (let [message (parse line)]
        (case (:type message)
          "RECORD"
          (write-record (:stream message) (merge (:record message) data) nil nil)

          "SCHEMA"
          (log/info "schema" message)

          "STATE"
          (log/info "STATE"  message)
          )))))


(defmethod transform "exchange"
  [args]
  (let [config (load-config args)]
    (doseq [line (line-seq (java.io.BufferedReader. *in*))]
      (let [message (walk/keywordize-keys (parse line))]
        (case (:type message)
          "RECORD"
          (let [record (:record message)
                stream (:stream message)
                new-record (transform-map record transformation)]
            (write-record stream new-record nil nil)
            ;(log/info record)
            )
          

          "SCHEMA"
          (log/info "schema" message)

          "STATE"
          (log/info "STATE"  message)
          )))))


