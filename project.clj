(defproject etl "0.1.0-SNAPSHOT"
  :description "ETL for Clojure"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [cli-matic "0.3.3"]
                 [cheshire "5.8.1"]
                 [org.clojure/data.csv "0.1.4"]
                 [com.google.auth/google-auth-library-credentials "0.13.0"]
                 [com.google.cloud/google-cloud-pubsub "1.62.0"]
                 [say-cheez "0.1.1"]
                 [org.clojure/tools.logging "0.4.1"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :main etl.core
  :aot :all
  :plugins [[lein-bin "0.3.5"]
            [gorillalabs/lein-docker "1.3.0"]]
  :bin {:name "etl"}
  :docker {:image-name "nativekloud/etl"
         :tags ["%s" "latest"] ; %s will splice the project version into the tag
         :dockerfile "Dockerfile"
         :build-dir  "target"}
  )
