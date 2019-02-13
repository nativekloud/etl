(ns etl.pubsub.core
  (:require [etl.singer.core :refer [discover tap load-config load-catalog]]
            [etl.singer.encoding :refer [decode]]
            [etl.singer.messages :refer [write-record]]
            [etl.singer.catalog :refer [stream write-streams]]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import
   (com.google.pubsub.v1
    PubsubMessage
    ProjectTopicName
    ProjectSubscriptionName)
   (com.google.api.core
    ApiService$Listener
    ApiFuture)
   (com.google.cloud.pubsub.v1
    TopicAdminClient
    SubscriptionAdminClient
    Publisher
    AckReplyConsumer
    MessageReceiver
    AckReplyConsumer
    Subscriber
    )
   com.google.protobuf.ByteString
   ))

(def state (atom {}))

;;; Private
(defn list-topics [project_id]
  (let [client (TopicAdminClient/create)]
    (mapv #(.getName %) (iterator-seq (.iterator
                                       (.iterateAll
                                        (.listTopics client project_id)))))))

(defn list-subscriptions [project_id]
  (let [client (SubscriptionAdminClient/create)]
    (mapv #(.getName %) (iterator-seq (.iterator
                                       (.iterateAll
                                        (.listSubscriptions client project_id)))))))


(defn publish-async 
  "publish async returns future"
  [project_id topic msg]
  (let [topic (ProjectTopicName/of project_id topic)
        publisher (.build (Publisher/newBuilder topic))
        data  (.build (.setData (PubsubMessage/newBuilder) (ByteString/copyFromUtf8 msg)))]
    (try (.publish publisher data)
         (catch Exception e (prn "handle this ..."))
         (finally (if publisher (.shutdown publisher))))))

(defn subscribe
  "pull subscription and asynchronously pull messages from it."
  [project_id subscription callback stream]
  (let [subscription (ProjectSubscriptionName/of project_id subscription)
        reciver (reify MessageReceiver
                  (^void receiveMessage [_ ^PubsubMessage msg ^AckReplyConsumer consumer]
                   (callback  (.toStringUtf8 (.getData msg)) stream)
                    (.ack consumer)))
        subscriber (.build (Subscriber/newBuilder subscription reciver))]

    (.awaitRunning (.startAsync subscriber))
    subscriber)
  )

(defn pubsub-message->record [m s]
  (write-record s (decode m) nil nil))

(defn stream->subscription-info [s]
  [(nth (str/split s #"/") 1) (last (str/split s #"/"))])

(defn subscribe-stream [stream]
  (let [[project_id subscription] (stream->subscription-info stream) ]
    (subscribe project_id subscription pubsub-message->record stream)))

(defn subscribe-streams [streams]
  (mapv #(subscribe-stream  (:stream %)) streams))

(defn subscription->stream [s]
  (stream s s {}))


(defn periodically
  [f interval]
  (doto (Thread.
          #(try
             (while (not (.isInterrupted (Thread/currentThread)))
               (Thread/sleep interval)
               (f))
             (catch InterruptedException _)))
    (.start)))

;;; Singer API

(defmethod discover "pubsub"
  [args]
  (let [config (load-config args)
        project_id (:project_id config)
        streams (mapv #(subscription->stream %) (list-subscriptions (str "projects/" project_id)))]
    (write-streams streams)
    ))

(defmethod tap "pubsub"
  [args]
  (let [config (load-config args)
        catalog (load-catalog args)
        streams (:streams catalog)
        subs (subscribe-streams streams)
        cancel (periodically (fn [] (mapv #(log/info (.toString %)) (:subs @state))) 30000)]
    (log/info "Replicating data from Google PubSub." subs)
    (swap! state assoc :subs subs)
    ;; keep tap open
    ;; TODO : exit cleanly with Ctrl + C
    (while true ())))
