(ns etl.msgraph.client
  (:require [clj-http.client :as http]
            [clj-http.conn-mgr :as conn-mgr]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+ try+]]
            [safely.core :refer [safely]]
            ))

;;; Microsoft Graph API URL's
;;; https://docs.microsoft.com/en-us/graph/use-the-api
;;; https://graph.microsoft.com/{version}/{resource}?query-parameters

(def settings (atom  {:params { :client_id     ""
                                :client_secret ""
                                :scope         ["https://graph.microsoft.com/.default"]
                                :grant_type    "client_credentials"
                                }
                       :tenant_id        ""
                       :token nil}))

;;; Settings



(defn set-client_id! [client_id]
  (swap! settings assoc-in [:params :client_id] client_id))

(defn set-client_secret! [client_secret]
  (swap! settings assoc-in [:params :client_secret] client_secret))

(defn set-tenant_id! [tenant_id]
  (swap! settings assoc-in [:tenant_id] tenant_id))


(defn dump-settings []
  (print "Settings atom: " @settings))

;;; API


(def version "v1.0")

(defn trunc
  [s n]
  (subs s 0 (min (count s) n)))


(def api-base-url (str  "https://graph.microsoft.com" "/" version))

(defn build-url [path]
  (str api-base-url path))

(def cm (conn-mgr/make-reusable-conn-manager {:timeout 5 :threads 20}))

(defn- api-token-url [tenant_id]
  (str "https://login.microsoftonline.com/" tenant_id "/oauth2/v2.0/token"))


(defn get-token []
  (:body (http/post (api-token-url (:tenant_id @settings))
                    {:form-params  (:params @settings)
                     :as                 :json
                     :timeout            5
                     :connection-manager cm
                     :throw-exceptions   true})))

(defn set-token! []
  (swap! settings assoc-in [:token]  (get-token)))

(defn set-params! [config]
  (set-client_id! (get-in config [:params :client_id]))
  (set-client_secret! (get-in config [:params :client_secret]))
  (set-tenant_id! (:tenant_id config))
  (set-token!))

;; API clj-http
(defn call-api [url]
  (safely
    (try+
   (log/info "Requesting: " (trunc url 200) "...")
   (http/get url {:oauth-token        (:access_token (:token @settings))
                  :as                 :json
                  :debug              false
                  :throw-exceptions   true
                  :connection-manager cm
                  })
                        ;
   (catch [:status 404] {:keys [request-time headers body]}
     (log/warn "404" request-time headers body))
   (catch Object _
     (log/error (:throwable &throw-context) "unexpected error")
     (throw+)))
  
   :on-error
   :max-retry :forever
   :track-as "etl.msgraph.client"
   :log-stacktrace false
   )
  )


;; API call

(defn api-get
  "Calls msgraph API and handles paged results.
  Returns vector of results"
  [url]
  (let [url (str api-base-url url)]
    (loop [response (:body (call-api url))
           results  []]
      (if (nil? ((keyword "@odata.nextLink") response))
        (concat results (:value response))
        (recur (:body (call-api ((keyword "@odata.nextLink") response)))
               (concat results (:value response)))))))

(defn api-get-callback
  "Calls msgraph API and handles paged results.
  Returns vector of results"
  [url user fn]
  (let [url (str api-base-url url)]
    (loop [response (:body (call-api url))
           results  (fn user (:value response))]
      (if (nil? ((keyword "@odata.nextLink") response))
        (fn user (:value response))
        (recur (:body (call-api ((keyword "@odata.nextLink") response)))
               (fn user (:value response)))))))


(defn api-get-delta
  "Delta query enables applications to discover newly created, updated, or deleted
  entities without performing a full read of the target resource with every request.
  Microsoft Graph applications can use delta query to efficiently synchronize changes
  with a local data store.
  See docs at https://docs.microsoft.com/en-us/graph/delta-query-overview
  Returns map {:resuls [] :deltaLink url} " 
  [url]
  (loop [response (:body (call-api url))
         results  []]
    (if (nil? ((keyword "@odata.nextLink") response))
      {:results  (concat results (:value response))
       :deltaLink ((keyword "@odata.deltaLink") response)} 
      (recur (:body (call-api ((keyword "@odata.nextLink") response)))
             (concat results (:value response))))))


(defn api-get-delta-callback
  "Delta query enables applications to discover newly created, updated, or deleted
  entities without performing a full read of the target resource with every request.
  Microsoft Graph applications can use delta query to efficiently synchronize changes
  with a local data store.
  See docs at https://docs.microsoft.com/en-us/graph/delta-query-overview
  Returns map {:resuls [] :deltaLink url} " 
  [url fn]
  (loop [response (:body (call-api url))
         results  (fn (:value response))]
    (if (nil? ((keyword "@odata.nextLink") response))
      {:deltaLink ((keyword "@odata.deltaLink") response)}
      (recur (:body (call-api ((keyword "@odata.nextLink") response)))
             (fn (:value response))))))





(comment

  (defn get-resource
  "Calls msgraph API and handles paged results.
  Returns vector of results"
  [url]
  (let [url (str api-base-url url)]
    (loop [response (:body (call-api url))
           results  []]
      (if (nil? ((keyword "@odata.nextLink") response))
        (concat results (:value response))
        (recur (:body (call-api ((keyword "@odata.nextLink") response))) (concat results (:value response)))))))
  

  (def resource {:users {:get   "/users"
                         :delta "/users/delta"}
                 :groups {:get   "/groups"
                          :delta "/users/delta"}
                 :mailFolders {:get "/users/%s/mailFolders"}
                 :messages {:get "/users/%s/mailFolders/%s/messages"}})

  (set-token!)
  
  (def users  (get-resource (get-in resource [:users :get])))

  (let [user (first (drop 110 users))]
    (:id user)
    (get-resource (format  (get-in resource [:mailFolders :get]) (:id user)))
    )
  

  
  
  (get-in resource [:users :get])
  
  @settings
  
  (get-token)
  
  (format "test %s", :et)
  
  
  
  
  )
