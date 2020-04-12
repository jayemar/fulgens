(defproject fulgens "0.2.0-SNAPSHOT"
  :description "Pandas-inspired library for working with table-style data"
  :url "https://github.com/jayemar/fulgens"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.csv "1.0.0"]
                 [org.clojure/data.json "1.0.0"]]
  :repl-options {:init-ns jayemar.fulgens.core})
