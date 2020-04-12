(ns jayemar.fulgens.core-test
  (:require [clojure.test :refer :all]
            [jayemar.fulgens.core :refer :all]))

(def csv-file "train.csv")

(def df (read-csv csv-file))

(deftest reader-test
  (testing "Reading CSV file into a valid DataFrame"
    (is (and (not (empty? df)) (all-maps? df)))))

(deftest df-shape
  (testing "Verify the proper shape of the test df"
    (is (= '(17324 8) (shape df)))))

(deftest iloc-test
  (testing "Able to retrieve df subsets by indices"
    (is (= 2 (count (iloc df 3 5))))
    (is (= 3 (count (iloc df -1 3))))
    (is (= 3 (count (iloc df -4 -1))))
    (is (= 17000 (count (iloc df 324 -1))))
    (is (= "4" (:Id (iloc df 3))))))
