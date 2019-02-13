(ns etl.msgraph.core
  (:require [clojure.tools.logging :as log]
            [etl.msgraph.client :refer [settings set-params! dump-settings]]
            [etl.singer.catalog :refer [stream]]
            [etl.singer.core :refer [discover load-catalog load-config]]))

;;; Singer

(defmethod discover "msgraph"
  [args]
  (let [config (load-config args)
        catalog (load-catalog args)]
    ;; set params from config file
    (log/info config)
    (set-params! config)
    ;; get users into atoms
   ; (set-users!)
    ;; Users stream 
    ;(assoc (stream "users" "users" {}) :metadata {:path ""})
    ;;(dump-settings)
    ))


