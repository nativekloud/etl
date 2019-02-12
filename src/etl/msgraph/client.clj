(ns etl.msgraph.client
  (:require [clj-http.client :as http]
            [clj-http.conn-mgr :as conn-mgr]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+ try+]]))

;;; Microsoft Graph API URL's

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


;;; API

(def version "v1.0")

(def api-base-url (str  "https://graph.microsoft.com" "/" version))

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

;; API clj-http

(defn call-api [url]
  (try+
   (http/get url {:oauth-token        (:access_token (:token @settings))
                  :as                 :json
                  :debug              false
                  :throw-exceptions   true
                  :connection-manager cm
                  })
   (catch [:status 403] {:keys [request-time headers body]}
     (log/warn "403" request-time headers))
   (catch [:status 401] {:keys [request-time headers body]}
     (log/warn "401" request-time headers))
   (catch [:status 400] {:keys [request-time headers body]}
     (log/error "400" body))
   (catch [:status 404] {:keys [request-time headers body]}
     (log/warn "404" request-time headers))
   (catch [:status 503] {:keys [request-time headers body]}
     (log/warn "503" request-time headers))
   (catch [:status 504] {:keys [request-time headers body]}
     (log/warn "504" request-time headers))
   (catch Object _
     (log/error (:throwable &throw-context) "unexpected error")
     (throw+))))


;; ;;  api fetch paged results

(defn api-get [url]
  (let [url (str api-base-url url)]
    (loop [response (:body (call-api url))
           results  []]
      (if (nil? ((keyword "@odata.nextLink") response))
        (concat results (:value response))
        (recur (:body (call-api ((keyword "@odata.nextLink") response))) (concat results (:value response)))))))

(defn api-get-delta
  "Delta query enables applications to discover newly created, updated, or deleted
  entities without performing a full read of the target resource with every request.
  Microsoft Graph applications can use delta query to efficiently synchronize changes
  with a local data store.
  See docs at https://docs.microsoft.com/en-us/graph/delta-query-overview" 
  [url]
  (loop [response (:body (call-api url))
         results  []]
    (if (nil? ((keyword "@odata.nextLink") response))
      {:results  (concat results (:value response))
       :deltaLink ((keyword "@odata.deltaLink") response)} 
      (recur (:body (call-api ((keyword "@odata.nextLink") response)))
             (concat results (:value response))))))



;;; Users
;;; https://docs.microsoft.com/en-us/graph/api/resources/users?view=graph-rest-1.0

(defn get-users
  "Lists users in the organization."
  []
  (api-get "/users?$top=999"))

(defn set-users!
  "Set users in state atom"
  []
  (swap! settings assoc-in [:users]  (get-users)))

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


;;; Groups

(defn get-groups []
  (api-get "/groups?$top=999"))

(defn set-groups! []
  (swap! settings assoc-in [:groups]  (get-groups)))

(comment
  (set-token!)
  (set-users!)
  (get-user-events (nth (:users @settings) 111))
 
  )

