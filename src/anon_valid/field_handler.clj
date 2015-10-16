;; Copyright (c) 2015 József Tiborcz, All rights reserved.
;;
;; The use and distribution terms for this software are covered by
;; the Eclipse Public License 1.0
;; (http://opensource.org/licenses/eclipse-1.0.php) which can be
;; found in the file epl-v10.html at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be
;; bound by the terms of this license.  You must not remove this
;; notice, or any other, from this software.
(ns anon-valid.field-handler)

(require '[bultitude.core :as b])

(defn load-definitions []
  (apply require (cons :reload-all (b/namespaces-on-classpath :prefix "anon-valid.sensitive-fields"))))

(def s-fields
  (atom #{}))

(def s-data
  (atom {}))

(defn sensitive-fields
  [& args]
  (if (seq? args) 
    (swap! s-fields #(apply conj %1 %2) args)))

(defn sensitive-data
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

(defn s-data*data-names-by-max
  "Returns data names which contains sensitive data definition of which length is up to max."
  ([data-name max-length] 
   (let [result (filter #(<= (count (second %)) max-length) (partition 2 (@s-data data-name)))]
     (if (empty? result) nil data-name)))
  ([max-length]
   (let [result (map #(s-data*data-names-by-max % max-length) (keys @s-data))]
     (remove nil? result))))

(defn s-data*filter-values-by-max
  "Returns a seq of sensitive values not longer than max-length. 
  If data-name is nil the function behaves as if called with max-length parameter only."
  ([data-name max-length]
   (if data-name
     (let [result (filter #(<= (count (second %)) max-length) (partition 2 (@s-data data-name)))]
       (if (empty? result) nil result))
     (s-data*filter-values-by-max max-length)))
  ([max-length]
   (let [result (apply concat (map #(s-data*filter-values-by-max % max-length) (keys @s-data)))]
     (if (empty? result) nil result))))

