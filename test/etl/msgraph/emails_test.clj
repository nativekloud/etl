(ns etl.msgraph.emails-test
  (:require [etl.msgraph.emails :refer :all]
            [clojure.test :refer  :all]
            [clojure.tools.logging :as log]))


(deftest has-mail?-test
  (testing "has-mail? filter")
  (let [users    [{:name "test" :mail "test@email.com"}
                  {:name "test2" :mail ""}
                  {:name "test3" :mail nil}]
        withMail (has-mail? users)]
    (is (= 1 (count withMail)))))

(deftest mailFolders-test
  (testing "mailFolders returns right URL")
  (with-redefs [etl.msgraph.client/api-get (fn [f] f )]
    (let [user {:id "12345"}
          mailFolders (mailFolders user)]
      (is (= mailFolders "/users/12345/mailFolders")))))

