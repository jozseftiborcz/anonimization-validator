;; This is a sample of sensitive field/data definition file.
;; The file is evaluated in a context where the following functions are available:
;; * sensitive-data
;; * sensitive-fields
(sdata-definition "General sensitive data" :version "0.1")

(sensitive-fields "name" "phone" "address" "card(?:_no)?" "birth" "family" "first_?name" "last_?name" "expiration_date" "CVC" )

(sensitive-data "Sensitive #1" :like "visa" "password")
(sensitive-data "Sensitive #1" :exact "budapest")
(sensitive-data "Sensitive #2" :exact "1234567890")
(sensitive-data "JobTitle" :like "vice president")
