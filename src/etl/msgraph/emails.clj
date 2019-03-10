(ns etl.msgraph.emails
  (:require [clojure.tools.logging :as log]
            [etl.msgraph.client :refer [api-get]]))


(defn has-mail? [users]
  (filter #(not-empty (:mail %)) users))

;; Folders

(defn mailFolders [user]
  (api-get (str "/users/" (:id user) "/mailFolders")))

(defn childFolders [user folder]
  (api-get (str "/users/" (:id user) "/mailFolders/" (:id folder) "/childFolders")))


(defn get-all-childFolders [user folders]
  (loop [with-children (filter #(not=  0 (:childFolderCount %)) folders)
         response (flatten (filter not-empty (map #(childFolders user %) with-children)))
         children []]
    (if (empty? with-children)
      (concat children response)
      (recur (filter #(not= 0 (:childFolderCount %)) response)
             (flatten (filter not-empty (map #(childFolders user %) with-children)))
             (concat children response) ))))

(defn get-user-folders [user]
  (let [folders (mailFolders user)
        children (get-all-childFolders user folders)
        all (concat folders children)]
    all))



;; Messages

(defn messages [user folder]
  (api-get (str "/users/" (:id user) "/mailFolders/" (:id folder) "/messages?$top=999")))
