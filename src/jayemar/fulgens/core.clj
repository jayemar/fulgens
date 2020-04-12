(ns jayemar.fulgens.core
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.edn :as edn]))

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

;; TODO: Should expand this check; maybe use spec?
(defn df?
  "Checks whether or not it's a DataFrame"
  [d]
  (all-maps? d))

(defn loc
  "Return DataFrame with colums specified in cols"
  [df & cols]
  (mapv #(select-keys % cols) df))

(defn iloc
  "Return item at idx or range from start to stop"
  ([df start stop]
   (cond
     (and (< start 0) (< stop 0)) (let [c (count df)]
                                    (subvec df (+ c start 1) (+ c stop 1)))
     (< start 0) (subvec df 0 stop)
     (< stop 0) (subvec df start)
     :else (subvec df start stop)))
  ([df idx]
   (first (subvec df idx (inc idx)))))

;; TODO: Finish this function
(defn- fillna
  "Fill nil fields with the result of a function"
  [df x]
  df)

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

(defn- to-num
  "Convert a value to a number"
  [val]
  (if (string? val) (edn/read-string val) val))

(defn- to-int
  "Convert a value to an integer"
  [val]
  (int (to-num val)))

(defn- to-float
  "Convert a value to an integer"
  [val]
  (float (to-num val)))

(defn- dict-retype
  "Change the dtype of a values in a dict based on type-map where type-map
  is of the form {:id type}"
  [dict type-map]
  (reduce
   (fn [m [k v]]
     (let [func (type-map k)]
       (cond
         (= func int) (assoc m k (to-int v))
         (= func float) (assoc m k (to-float v))
         (= func str) (assoc m k (str v))
         :else (assoc m k v))))
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

(defn read-csv
  "Reads CSV file filename and returns a map of the data"
  [filename]
  (with-open [f (io/reader filename)]
    (DataFrame
     (doall
      (csv/read-csv f)))))

(defn save-to-json
  "Save a data structure to filename as JSON data"
  [obj filename]
  (with-open [f (io/writer filename)]
    (json/write obj f)))

(defn read-from-json
  "Read a data structure from filename as JSON data"
  [filename]
  (with-open [f (io/reader filename)]
    (json/read f)))
