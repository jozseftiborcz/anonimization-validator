(ns anon-valid.sensitive-fields.person_english_general)

(require '[anon-valid.field-handler :as fh])

(fh/sensitive-fields "name" "phone" "product_id")

