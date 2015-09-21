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
  (apply require (b/namespaces-on-classpath :prefix "anon-valid.sensitive-fields")))

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


(defn s-data*filter-max 
  ([data-name max-length] 
   (let [result (filter #(<= (count (second %)) max-length) (partition 2 (@s-data data-name)))]
     (if (empty? result) nil result)))
  ([max-length]
   (let [result (apply concat (map #(s-data*filter-max % max-length) (keys @s-data)))]
     (if (empty? result) nil result))))

;;(defn s-data*min-length [data-name]
;;  (min (map count (remove keyword? (@s-data data-name)))))
