;; Copyright (c) 2015 József Tiborcz, All rights reserved.
;;
;; The use and distribution terms for this software are covered by
;; the Eclipse Public License 1.0
;; (http://opensource.org/licenses/eclipse-1.0.php) which can be
;; found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be
;; bound by the terms of this license.  You must not remove this
;; notice, or any other, from this software.
(ns anon-valid.field-handler
  (:require [bultitude.core :as b]
            [clojure.string :as s]
            [clojure.tools.logging :as log] 
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import (java.io PushbackReader) ))
  ;(:import (java.io PushbackReader) (java.net ConnectException)))

(def s-fields
  (atom #{}))

(def s-data
  (atom {}))

(def ^:dynamic *sdata-coll* nil) ;; actual sdata collection 

(defn load-definitions 
  "Loads definition from standard namespace"
  []
  (apply require (cons :reload-all (b/namespaces-on-classpath :prefix "anon-valid.sensitive-fields"))))

; forms allowed in an sdata file
(def sdata-file-forms #{'sensitive-data 'sensitive-fields}) 

(defn- check-sdata-forms
  ([rdr]
    (let [f (read rdr false ::done)]
      (if (and (list? f) (= 'sdata-definition (first f)))
        (check-sdata-forms rdr true)
        (do
          (log/error "Invalid form" f)
          (log/error "Expected (sdata-definition \"coll-name\")")
          false))))
  ([rdr no-header]
    (let [f (read rdr false ::done)]
      (if (= ::done f) 
        true
        (if (and (list? f) (sdata-file-forms (first f)))
          (recur rdr true)
          (do (log/error "Expected one of" (s/join "," sdata-file-forms "got" f)) false))))))

(defn check-sdata-file
  "Checks if the file is a valid sdata definition"
  [file-name]
  (with-open [r (PushbackReader. (io/reader file-name))]
    (check-sdata-forms r)))
      
(defn load-sdata-file 
  "Loads and evaluate an sdata file"
  [file]
  (if (check-sdata-file file)
    (let [ns-str (str "(ns anon-valid.sdata." (.getName (io/file file)) ")")
          require-str (str "(use 'anon-valid.field-handler)")]
      (load-string (str ns-str require-str (slurp file)))
      true)
    false))

(defn load-sdata
  "Loads sensitive data definition from files or from sdata server (url)"
  [dir-or-file]
  (if (.startsWith dir-or-file "http")
    (if-let [result (try (edn/read-string (slurp dir-or-file))
                      (catch java.net.ConnectException e#
                        nil))]
      (do
        (reset! s-data (result :s-data))
        (reset! s-fields (result :s-fields))
        (log/info "loading sdata from url" dir-or-file)
        (list true))
      (do
        (log/error "Cannot load sdata from url")
        (list false)))
    (for [f (file-seq (io/file dir-or-file))
          :when (and (.isFile f) (.canRead f) (.endsWith (.getName f) ".sdata"))]
      (load-sdata-file f))))

(defn sdata-definition
  "Namespace of data collection definitions. It must be the first non-comment form of sdata files."
  [coll-name & args ]
  (def ^:dynamic *sdata-coll* coll-name)
  (let [version (:version (apply hash-map args))
        version (if version (str " (version " version ")") "")]
    (log/info (str "loading sdata '" coll-name "'" version "..."))))

(defn sensitive-fields
  "Defines field name patterns suspected containing sensitive data."
  [& args]
  (if (seq? args) 
    (swap! s-fields #(apply conj %1 %2) args)))

(defn sensitive-data
  "This defines a class of sensitive data to search for. Data-name is the name of the class as will be reported in 
  hits. Match-type defines the method as values will be compared to actual field values."
  [data-name match-type & args]
  (let [mt-args (flatten (map vector (repeat match-type) args))
        old-args (@s-data data-name)] 
    (swap! s-data assoc data-name (concat old-args mt-args))))

(def not-nil? (complement nil?))

(defn sensitive-field?
  [fld]
  {:pre [(> (count @s-fields) 0)]}
  (not-nil? (some #(re-find (re-pattern %) fld) @s-fields)))

;;(defn s-data-length []
;;  (map s-data*min-length (keys s-data)))

(defn pattern? [s] (= java.util.regex.Pattern (type s)))

(defn s-data*data-names-by-max
  "Returns data names which contains sensitive data definition of which length is up to max."
  ([data-name max-length] 
   (let [result (filter #(or (pattern? (second %)) 
                             (<= (count (second %)) max-length)) (partition 2 (@s-data data-name)))]
     (if (empty? result) nil data-name)))
  ([max-length]
   (let [result (map #(s-data*data-names-by-max % max-length) (keys @s-data))]
     (remove nil? result))))

(defn s-data*filter-values-by-max
  "Returns a seq of sensitive value types not longer than max-length. 
  If data-name is nil the function behaves as if called with max-length parameter only."
  ([data-name max-length]
   (if data-name
     (let [result (filter #(<= (count (second %)) max-length) (partition 2 (@s-data data-name)))]
       (if (empty? result) nil result))
     (s-data*filter-values-by-max max-length)))
  ([max-length]
   (let [result (apply concat (map #(s-data*filter-values-by-max % max-length) (keys @s-data)))]
     (if (empty? result) nil result))))

(defn s-data*match-sample
  "Returns a match from data-name which matches one of the pattern from sample, or nil otherwise."
  [samples data-name]
  (let [match-one (fn[match-type pattern sample] 
                    (case match-type
                      :exact (re-matches (re-pattern pattern) sample)
                      :like (re-find (re-pattern pattern) sample)))
        match-any (fn[sample]
                    (not (empty? (filter (fn[[mt p]] (not (empty? (match-one mt p sample)))) (partition 2 (@s-data data-name))))))]
    (take 5 (filter match-any samples))))

