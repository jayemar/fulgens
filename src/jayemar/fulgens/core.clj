(ns jayemar.fulgens.core
  (:require [clojure.data.csv :as csv]
            [clojure.string :as str]))

;; Don't print all of huge values
;; *print-level* may also be of interest
(set! *print-length* 10)

(def dummy 2)

(def proj-home "/home/jayemar/projects/covid19/covid-forecast/week1/global/")

(defn get-csv
  "Reads CSV file filename and returns a map of the data"
  [filename]
  (csv/read-csv (slurp (apply str [proj-home filename]))))

(def train-csv (get-csv "train.csv"))

(def test-csv (get-csv "test.csv"))

(defn str->int
  "Convert a string to the integer that had been stringified"
  [val]
  ;; (int (Float/parseFloat val))
  (int (read-string val)))

(defn str->float
  "Convert a string to the float that had been stringified"
  [val]
  ;; (int (Float/parseFloat val))
  (float (read-string val)))

;; Could use clojure.set/rename-keys with this
(def rename-map
  {"Id" "id"
   "ForecastId" "id"
   "Province/State" "state"
   "Country/Region" "country"
   "Date" "date"
   "ConfirmedCases" "confirmed"
   "Fatalities" "fatalities"
   "Lat" "lat"
   "Long" "lon"})

(def convert-type-map
  {:id str->int
   :lat str->float
   :lon str->float
   :confirmed str->int
   :fatalities str->int})

(defn csv-rename-cols
  "Rename column headings in a csv-style list using {old new}"
  [csv-list name-map]
  (cons
   (mapv #(if (name-map %) (name-map %) %) (first csv-list))
   (rest csv-list)))


(defn retype-dict-bad
  "Change the dtype of a values in a dict based on type-map where type-map
  is of the form {:id type-fn}"
  [dict type-map]
  (mapv #(if (type-map (key %))
           (hash-map (key %) ((type-map (key %)) (val %)))
           (hash-map (key %) (val %)))
        dict))

(defn retype-dict-multiple
  "Change the dtype of a values in a dict based on type-map where type-map
  is of the form {:id type-fn}"
  [dict type-map]
  (mapv
   #(if (dict (key %))
      (update dict (key %) (val %))
      dict)
   type-map))

;; use with convert-type-map
(defn retype-dict
  "Change the dtype of a values in a dict based on type-map where type-map
  is of the form {:id type-fn}"
  [dict type-map]
  (reduce 
   (fn [r [k v]]
     (if (type-map k)
      (update dict k v)
      dict))
   {}
   dict))

(defn update-values [m f & args]
  (reduce (fn [r [k v]] (assoc r k (apply f v args))) {} m))

(defn df-retype
  "Convert the type of column in a DataFrame using {:id dtype}"
  [df type-map]
  (mapv #(retype-dict % type-map) df))

(defn loc
  "Return DataFrame with colums specified in cols"
  [df & cols]
  (map (apply juxt cols) df))

(defn iloc
  "Return item at idx or range until stop"
  ([df idx stop]
   (subvec df idx stop))
  ([df idx]
   (subvec df idx (inc idx))))

;; For multi-level could use clojure.walk/keywordize-keys
(defn keywordize-keys
  "Convert hmap keys into keywords, replacing spaces with underscores"
  [hmap]
  (zipmap
   (map #(keyword (str/replace % #" " "_")) (keys hmap))
   (vals hmap)))

(defn DataFrame
  "Convert a csv-style list into a pandas-like map (DataFrame)"
  [csv-list]
  (let [keys (map #(keyword %) (first csv-list))
        data (rest csv-list)]
    (mapv #(zipmap keys %) data)))

(def train-df
  (DataFrame (csv-rename-cols train-csv rename-map)))

(def test-df
  "Attempt using the thread approach"
  (-> test-csv
      (csv-rename-cols rename-map)
      DataFrame))

(defn df-cols
  "Show a list of the columns in a DataFrame"
  [df]
  (keys (first df)))

(defn map-ordered-vals
  "Order a map be values in descending order"
  [hmap]
  ( ))

(def freq-map (keywordize-keys (frequencies (map #(:country %) train-df))))

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

(def freq-map-sorted (map-val-sort freq-map))

(def freq-map-rsorted (map-val-rsort freq-map))
