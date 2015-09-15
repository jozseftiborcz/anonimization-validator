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

(def field-definitions
  (atom #{}))

(def data-definitions
  (atom []))

(defn sensitive-fields
  [& args]
  (if (seq? args) 
    (swap! field-definitions #(apply conj %1 %2) args)))

(defn sensitive-data
  [data-name match-type & args]
  (swap! data-definitions #(apply conj %1 [data-name match-type args])))

(def not-nil? (complement nil?))

(defn sensitive-field?
  [fld]
  {:pre [(> (count @field-definitions) 0)]}
  (not-nil? (some #(re-find (re-pattern %) fld) @field-definitions)))

(defn data-max-length [values]
  (max (map count values)))

(defn data-min-length [values]
  (min (map count values)))

