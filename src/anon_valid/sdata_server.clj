(ns anon-valid.sdata-server
  (:gen-class)
  (:require [clojure.tools.logging :as log] 
            [clojure.pprint :as pp]
            [clojure.string :as string]
            [clojure.term.colors :refer :all]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.util.response :as r]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [anon-valid.field-handler :as fh]))

(defroutes app-routes
  (GET "/" request (do 
    (log/info "Serving sdata to" (request :remote-addr))
    (-> (r/response (pr-str {:s-data @fh/s-data :s-fields @fh/s-fields}))
      (r/content-type "text/plain; charset=utf-8"))))
  (route/not-found "Not Found"))

(def app
  (wrap-defaults app-routes site-defaults))

(defn start-sdata-server
  "This starts a sensitive data providing service."
  [server-port]
  (log/info "Starting sensitive-data serving server at port" server-port)
  (run-jetty app {:port server-port})
  (log/info "Shutting down server"))
