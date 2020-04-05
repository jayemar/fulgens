(ns jayemar.fulgens.core
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.string :as str]
            [msgpack.core :as msg]))

;; TODO: Add TESTS

(defn- csv-rename-cols
  "Rename column headings in a csv-style list using {old new}"
  [csv-list name-map]
  (cons
   (mapv #(if (name-map %) (name-map %) %) (first csv-list))
   (rest csv-list)))

(defn all-maps?
  "Determines whether all items in seq are hashmaps"
  [s]
  (empty? (filter #(not (map? %)) s)))

(defn all-lists?
  "Determines whether all items in seq are seqs or vectors"
  [s]
  (empty? (filter #(and (not (vector? %)) (not (seq? %))) s)))

;; For multi-level could use clojure.walk/keywordize-keys
(defn- keywordize-keys
  "Convert hmap keys into keywords, replacing spaces with underscores"
  [hmap]
  (zipmap
   (map #(if (not (keyword? %))
           (keyword (str/replace % #" " "_"))%)
        (keys hmap))
   (vals hmap)))

;; TODO: Allow for passing in column name map at time of creation
(defn DataFrame
  "Convert a csv-style list into a pandas-like map (DataFrame)"
  [sq]
  (if (all-lists? sq)
    (let [keys (map #(keyword %) (first sq))
          data (rest sq)]
      (mapv #(zipmap keys %) data))
    ;; If the first item is a map, we're assuming it's already DataFrame
    (mapv keywordize-keys sq)))

;; TODO: Should expand this check.  All maps should have the same keys and dtypes
(defn df?
  "Checks whether or not it's a DataFrame"
  [d]
  (all-maps? d))

(defn loc
  "Return DataFrame with colums specified in cols"
  [df & cols]
  ;; (map (apply juxt cols) df)
  (mapv #(select-keys % cols) df))

(defn iloc
  "Return item at idx or range until stop"
  ([df idx stop]
   (subvec df idx stop))
  ([df idx]
   (subvec df idx (inc idx))))

(defn shape
  "Return the number of rows and columns"
  [df]
  (seq [(count df) (count (first df))]))

(defn columns
  "Show a list of the columns in a DataFrame"
  [df]
  (if (df? df)
    (keys (first df))))
(def cols columns)

;; Could use clojure.set/rename-keys
(defn rename
  "Rename columns in DataFrame using hash-map"
  [df m]
  (map #(reduce
         (fn [r [k v]]
           (if (k m)
             (assoc r (k m) v)
             (assoc r k v)))
         {}
         %)
       df))

(defn- dict-retype
  "Change the dtype of a values in a dict based on type-map where type-map
  is of the form {:id type-fn}"
  [dict type-map]
  (reduce
   (fn [m [k v]]
     (if (type-map k)
       (assoc m k ((type-map k) v))
       (assoc m k v)))
   {}
   dict))

(defn astype
  "Convert the type of column in a DataFrame using {:id dtype}"
  [df type-map]
  (mapv #(dict-retype % type-map) df))

(defn map-val-sort
  "Sort a hash-map in decreasing order using the values"
  [hmap]
  (->> (interleave (keys hmap) (vals hmap))
       (partition 2)
       (map reverse)
       (sort-by first)
       (reverse) ;; this determines increasing or decreasing order
       (map reverse)
       (mapv #(hash-map (first %) (last %)))))

(defn map-val-rsort
  "Sort a hash-map in increasing order using the values"
  [hmap]
  (->> (interleave (keys hmap) (vals hmap))
       (partition 2)
       (map reverse)
       (sort-by first)
       (map reverse)
       (mapv #(hash-map (first %) (last %)))))

;; TODO: Use better IO handling operation(s)
(defn read-csv
  "Reads CSV file filename and returns a map of the data"
  [filename]
  (DataFrame (csv/read-csv (slurp filename))))

;; TODO: Implement this function, hopefully with proper IO handling
(defn to-msgpack
  "Write object to a file in seiralized msgpack format"
  [df filename]
  nil)
