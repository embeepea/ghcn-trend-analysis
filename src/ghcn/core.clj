(ns ghcn.core
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [incanter.stats :as stats]))

; Take a string, trim whitespace from both ends, and if what remains
; is the empty string, return nil, otherwise return the trimmed string.
(defn trim-and-nilify [s]
  (let [ts  (clojure.string/trim s)]
    (if (= ts "") nil ts)))

; Take a string which is a single line from the ghcnd-stations.txt file
; (see below for where this file comes from), and return a map containing
; the data from that line.
(defn station-map [s]
  {:id           (trim-and-nilify (subs s 0 11))
   :latitude     (trim-and-nilify (subs s 12 20))
   :longitude    (trim-and-nilify (subs s 21 30))
   :elevation    (trim-and-nilify (subs s 31 37))
   :state        (trim-and-nilify (subs s 38 40))
   :name         (trim-and-nilify (subs s 41 71))
   :gsn-flag     (trim-and-nilify (subs s 72 75))
   :hcn-crn-flag (trim-and-nilify (subs s 76 79))
   :wmo-id       (trim-and-nilify (subs s 80 84))})

; Takes the name of the ghcn stations.txt file, which is a copy of
; ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt,
; and returns a map whose keys are the station ids, and whose
; values are maps with the above keys (except :id)
(defn parse-stations-txt [file]
  (with-open [rdr (io/reader file)]
    (doall (into {}
                 (map (fn [st] [(:id st) (dissoc st :id)])
                      (map station-map (line-seq rdr)))))))

(def stations-meta (parse-stations-txt "data/stations/ghcnd-stations.txt"))

; stations-meta is now a map whose keys are station ids, and whose values
; are maps with the keys:
;     :latitude
;     :longitude
;     :elevation
;     :state
;     :name
;     :gsn-flag
;     :hcn-crn-flag
;     :wmo

; Take the name of a file containing edn data, and return the value that
; results from reading and evaluating that file.
(defn load-edn [filename]
  (with-open [r (io/reader filename)]
    (read (java.io.PushbackReader. r))))

(def stations-por-summary (load-edn "data/stations/summary.edn.txt"))

; Predicate for testing whether a station id is in the US
(defn in-us? [id] (not (nil? ((stations-meta id) :state))))

; Remove nil values from a collection:
(defn non-nils [vals] (filter (fn [v] (not (nil? v))) vals))

; Take a string of comma-separated fields, and return a vector
; of the fields as strings. Does not understand quoted fields,
; and does not trim any whitespace from the fields.
(defn comma-split-vector [s]
  (vec (clojure.string/split s #",")))

;;; The following function (parse-ghcn-single-element-csv) was
;;; part of some initial work that I did related to extracting
;;; data for a single variable and a single station from one of
;;; the ghcn yearly files.  It reads from files that I created
;;; manually using command-line tools and emacs, and is not
;;; intended to be part of an ongoing workflow, so I am
;;; commenting it out.
;;;
;;;  ; parse-ghcn-single-element-csv takes a string which is the name of (or path to) a
;;;  ; csv file containing GHCN data for a single element for a single station, and returns
;;;  ; a list of vectors containing the data in the file.  Each vector in the list will have
;;;  ; two elements: a string representing the date, and an integer which is the data file
;;;  ; for that date.  No scaling is done to the data; the integer data values are taken
;;;  ; verbatim from the data file.  Each line of the file should look like this (minus
;;;  ; the quotes):
;;;  ;     "USW00094728,20150101,TMAX,39,,,W,2400"
;;;  (defn parse-ghcn-single-element-csv [file]
;;;    (with-open [rdr (io/reader file)]
;;;      (doall (let [lines   (map comma-split-vector (line-seq rdr))]
;;;               (map (fn [v] [(v 1) (Integer/parseInt (v 3))]) lines)
;;;               ))))

; Parse-two-column-gzipped-csv takes a string which is the name of (or path to) a
; gzipped csv file containing two fields per line -- a date string and an integer.
; It returns a list of 2-element vectors containing the data; the dates are strings,
; and the data values are integers.  The input file should have lines that look
; like the following (minus the quotes):
;     "19090801,389"
(defn parse-two-column-gzipped-csv [file]
 (with-open [rdr (io/reader (java.util.zip.GZIPInputStream. (io/input-stream file)))]
   (doall (let [lines (map comma-split-vector (line-seq rdr))]
            (map (fn [v] [(v 0) (Integer/parseInt (v 1))]) lines)
            ))))

; Take a 8-character string in the format YYYYMMDD, and return the YYYY
; part, as a string:
(defn data-year [[date value]] (subs date 0 4))

; Convert celsius to Fahrenheit:
(defn fahrenheit [c] (+ (/ (* 9.0 c) 5.0) 32.0))

; Convert 10ths of degrees Celsius to Fahrenheit.  (The values in ghcn files
; are in 10ths of degrees Celsius.)
(defn scale-temp [tc] (fahrenheit (/ tc 10.0)))

; Take a collection of pairs consisting of [date,temp], where the temp
; values are (ints) in 10ths of degrees Celsius, and convert all the
; temps to Fahrenheit.  Return a collection having the same structure
; as the input collection, but in which the temps are all converted.
(defn scale-temp-array [data]
  (map (fn [[date temp]] [date (scale-temp temp)]) data))

; Determine whether or not `n` is divisible by `divisor`
(defn divisible-by? [n divisor] (= 0 (rem n divisor)))

; Is `year` a leap year?
(defn leap-year? [year]
  (if (divisible-by? year 400)
    true
    (if (divisible-by? year 100)
      false
      (divisible-by? year 4))))

; Some values needed for julian-day calculations:
(def month-days [31 28 31 30 31 30 31 31 30 31 30 31])
(def leap-month-days (assoc month-days 1 29))
(def cumulative-month-days (vec (drop-last (reductions + 0 month-days))))
(def cumulative-leap-month-days (vec (drop-last (reductions + 0 leap-month-days))))

; Returns an integer in the range 0-364 for non-leap years,
; or 0-365 for leap years, giving the index of the day
; in the year
(defn julian-day [datestring]
  (let [year  (Integer/parseInt (subs datestring 0 4))
        month (Integer/parseInt (subs datestring 4 6))
        day   (Integer/parseInt (subs datestring 6 8))
        cmd   (if (leap-year? year)
                cumulative-leap-month-days
                cumulative-month-days)]
    (+ (cmd (dec month)) (dec day))))

; Return the number of days in (int) `year`
(defn year-length [year] (if (leap-year? year) 366 365))

; Take a (int) `year` and a collection `data` consisting of pairs of
; [date (string), value (int)], and return a map representing the same
; data, in which the keys are years, and the value for each key is a
; vector giving the daily values for that year.
(defn year-data [year data]
  (reduce
   (fn [v [datestring val]] (assoc v (julian-day datestring) val))
   (vec (repeat (year-length year) nil))
   data))

; Takes a station id and a var ("TMAX" or "TMIN").  Reads the data for that station
; and var from the file "data/observations/STATION_ID/VAR.csv.gz", and returns a map
; whose keys are integer years, and whose values are arrays of integers giving the
; data values for that year.  The length of the value array will be 365, or 366 for
; leap years, with `nil` in positions corresponding to missing data.  For example:
;    {1976 [161 194 ... 183], ; data for 1976: Jan 1, Jan 2, ..., Dec 31
;     1952 [278 nil ...  94], ; data for 1952: Jan 1, Jan 2 (missing), ..., Dec 31
;     ...}
(defn data-year-map [id var]
  (let [data         (parse-two-column-gzipped-csv (str "data/observations/" id "/" var ".csv.gz"))
        y-data       (group-by (fn [[d v]] (Integer/parseInt (subs d 0 4))) data)
        y-data-pairs (map (fn [[year vals]] [year (year-data year vals)]) y-data)]
    (into {} y-data-pairs)
    ))

; Take a station id and a var ("TMAX" or "TMIN"), read the corresponding data
; from "data/observations/STATION_ID/VAR.csv.gz", and write out the converted
; data-year-map object to the file "data/observations/STATION_ID/VAR.edn".
(defn write-data-year-map [id var]
  (spit (str "data/observations/" id "/" var ".edn") (with-out-str (pr (data-year-map id var)))))

; The above function was used to create *.edn files corresponding to all the *.csv.gz
; files in data/observations.  Use the following function to load one of these *.edn
; files, which is faster than loading the *.csv.gz file:
(defn load-data-year-map [id var]
  (read-string (slurp (str "data/observations/" id "/" var ".edn"))))

; Return the total number of days in the period [year1,year2] (ints, inclusive):
(defn num-days-in-year-range [year1 year2] (apply + (map year-length (range year1 (inc year2)))))

; Take a station id and a data var ("TMAX" or "TMIN"), and two years (ints).
; Let PERIOD be the period of all days from the beginning of year1 to the end of year2.
; Return a floating point number between 0 and 1 which gives the ratio A/B, where
;   A = number of days in the PERIOD for which data for that variable and station exists
;   B = total number of days in PERIOD
(defn data-fraction [id var year1 year2]
  (let [data-year-map     (load-data-year-map id var)
        data-in-range     (filter (fn [[year vals]] (and (>= year year1) (<= year year2))) data-year-map)
        data-counts       (map (fn [[year vals]] (count (filter #(not (nil? %)) vals))) data-in-range)
        ]
    (/ (double (apply + data-counts)) (num-days-in-year-range year1 year2))))


; return the length of the longest consecutive sequence of `nil` values from `vals`:
(defn max-nil-run-length [vals]
  (second (reduce
           (fn [[cmax tmax] v]
             (if (nil? v)
               (let [n (inc cmax)] [n (max n tmax)])
               [0 tmax]))
           [0 0]
           vals)))

; Take a station id and a var ("TMAX" or "TMIN"), and return a map whose keys
; are years (ints), and whose value for each year is an int giving the longest
; stretch of consecutive missing days for data for that station/var in that year.
(defn longest-missing-stretch-map [id var]
  (let [data-map       (load-data-year-map id var)
        nil-run-maxer  (fn [[year vals]] [year (max-nil-run-length vals)])]
    (into {} (map nil-run-maxer data-map))))

; Take a station id, a var, and an int, and return the list of years
; for which the data for that station/var has a consecutive stretch
; of missing data of at most `longest-missing-stretch-length` days.
(defn good-years [id var longest-missing-stretch-length]
  (let [lmsm   (longest-missing-stretch-map id var)]
    (sort (map first (filter
                      (fn [[year stretch-length]] (<= stretch-length longest-missing-stretch-length))
                      lmsm)))))

(defn avg [vals] (/ (apply + vals) (double (count vals))))

(defn split-coords [points]
  [(mapv second points) (mapv first points)])

(defn station-latlon [id] 
  (let [station (stations-meta id)]
    [(Double/parseDouble (:longitude station))
     (Double/parseDouble (:latitude station))]))

(defn station-state-name [id] 
  (let [station (stations-meta id)]
    (str (:state station) " " (:name station))))


; Take two equal-length vectors, and return a new vector whose values are the
; averages of the values in the input vectors, and which contains nil in any
; position whether either input vector has a nil entry.
(defn avg-of-two-vecs [vec1 vec2]
  (vec (map
        (fn [v1 v2] (if (or (nil? v1) (nil? v2)) nil (avg [v1 v2])))
        vec1 vec2)))

; Take a station id and compute a data-year-map representing (TMAX+TMIN)/2.
; This reads TMAX and TMIN data from the station's *.edn files in
; data/observations.
(defn avg-data-year-map [id]
  (let [tmax-data-year  (load-data-year-map id "TMAX")
        tmin-data-year  (load-data-year-map id "TMIN")]
    (into {} (map
              (fn [year] [year (avg-of-two-vecs
                                (tmin-data-year year)
                                (tmax-data-year year))])
              (clojure.set/intersection
               (set (keys tmax-data-year))
               (set (keys tmin-data-year)))))))

; Compute and write out the TAVG.edn file containing the (TMAX+TMIN)/2 values
; for a station.
(defn write-avg-data-year-map [id]
  (spit (str "data/observations/" id "/TAVG.edn") (with-out-str (pr (avg-data-year-map id)))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; ; takes a data map of the sort created by parse-two-column-gzipped-csv, and returns
;;; ; a map whose keys are 4-digit years (as strings), and whose values are integers giving
;;; ; the number of days in each year for which a value is present in the data map
;;; (defn data-days [data]
;;;   (map 
;;;    (fn [[k v]] [k (count v)])
;;;    (group-by data-year data)))
;;; 
;;; ; takes a data map of the sort created by parse-two-column-gzipped-csv, and returns
;;; ; a list of 4-digit years (as strings) corresponding to the "focus" years of the data.
;;; ; A "focus" year is defined as a year between 1981 and 2010 (inclusive) for which
;;; ; there are at least 350 daily values for the year.
;;; (defn focus-years [data]
;;;   (sort (map first
;;;              (filter
;;;               (fn [[k v]]
;;;                 (and
;;;                  (>= (compare k "1981") 0)
;;;                  (<= (compare k "2010") 0)
;;;                  (>= v 350)
;;;                  ))
;;;               (data-days data))
;;;              )))
;;; 
;;; ; take a data map of the tmax sort, and return a map whose keys are years,
;;; ; and whose values are vectors of all the values for the year
;;; (defn year-vals [data]
;;;   (reduce
;;;    (fn [m [day temp]]
;;;      (update-in m [(subs day 0 4)] (fn [vals] (if (empty? vals) [temp] (conj vals temp)))))
;;;    {}
;;;    data))
;;; 
;;; (defn year-avgs [year-data]
;;;   (vec (sort-by (fn [[year val]] year)
;;;                 (mapv (fn [[year vals]] [(Integer/parseInt year) (scale-temp (avg vals))]) year-data)))
;;; )
;;; 
;;; ; trend-coefs takes
;;; ;             id: a station id, as a string, such as "USC00229743"
;;; ;            var: either "TMAX" or "TMIN"
;;; ;     first-year: first year of period to compute trend over, as a string, like "1981"
;;; ;      last-year: last year of period to compute trend over, as a string, like "2010"
;;; ; returns:
;;; ;   a vector of the coefs of a least-squares fit trend line; first element of vector is
;;; ;   intercept, second element is slope
;;; ; example:
;;; ;   (trend-coefs "USC00229743" "TMAX" "1981" "2010")
;;; ;     ; => [2035.3948184847832 -0.5476302776223747]
;;; (defn trend-coefs [id var first-year last-year]
;;;   (let [t           (parse-two-column-gzipped-csv (str "data/observations/" id "/" var ".csv.gz"))
;;;         filtered-t  (filter
;;;                      (fn [[day temp]]
;;;                        (and
;;;                         (>= (compare day (str first-year "0000")) 0)
;;;                         (<= (compare day (str last-year  "9999")) 0)))
;;;                      t)
;;;         ]
;;;     (:coefs (apply stats/linear-model (split-coords (year-avgs (year-vals filtered-t)))))))



"core.clj"
