(ns anon-valid.cache
  (:require [clojure.java.io :as io]
            [anon-valid.db :as db]))

(def the-cache (atom {}))

(defn load-cache 
  "Loads the cache file"
  [file]
  (reset! the-cache (read-string (slurp file))))

(defn write-cache
  "Writes the cache to file and creates a backup"
  [file]
  (let [bkp (io/file (str file ".bkp"))
        file (io/file file)]
    (if (.exists file)
      (io/copy file bkp))
    (spit file @the-cache)))

(defn- set-cache 
  ([args k-count v-count]
   (swap! the-cache assoc (take k-count args) 
          (take v-count (drop k-count args))))
  ([k v]
   (swap! the-cache assoc k v)))

(defn cache-x
  [k v]
  (swap! the-cache assoc k v))

(defn cache
  "Set cache from progress information. If called without arguments returns the full cache."
  ([[stage & args :as all-param]]
   (case stage
     :sensitive-field (set-cache all-param 4 1)
     :table-row-count (set-cache all-param 2 1)
     :sensitive-table (set-cache (first args) :sensitive-table)
     :not-sensitive-table (set-cache (first args) :not-sensitive-table)
     nil))
  ([] 
   the-cache))

(defn not-empty? 
  "Returns if cache is empty"
  [] (empty? @the-cache))

(defn sensitive-table
  "Returns cached result of table sensitivity, nil if result is not cached."
  [table-name]
  (get @the-cache table-name))

(defn get-cached
  "It checks if a value is in the cache. Returns nil if not in cache, value otherwise."
  [& k]
  (get @the-cache k))

(defn field-sensitive-to-data-name?
  "Checks if cache contains information about field sensitivity to data-name."
  [field data-name]
  (get-cached 
    :sensitive-field 
    (db/exact-table-name field) 
    (:column_name field) 
    data-name))

