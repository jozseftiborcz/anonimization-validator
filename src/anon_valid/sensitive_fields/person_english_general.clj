(ns anon-valid.sensitive-fields.person_english_general)

(require '[anon-valid.field-handler :as fh])

(fh/sensitive-fields "name" "phone" "address" "card(?:_no)?" "birth" "family" "first_?name" "last_?name" "expiration_date" "CVC" )

;;(fh/sensitive-type "BLOB" "CBLOB")

(fh/sensitive-data "Sensitive #1" :like "visa" "password")
(fh/sensitive-data "Sensitive #1" :exact "budapest")
(fh/sensitive-data "Sensitive #2" :exact "1234567890")
(fh/sensitive-data "JobTitle" :like "vice president")
;;(fh/anonimizator "JobTitle" :empty)
;;(fh/sensitive-data "Sens3" :regexp :any-of ".*bud.*")
;;(fh/sensitive-data "Sens3" :any-of :like "bud")
;;(fh/sensitive-data "Sens3" :every :like "bud")


