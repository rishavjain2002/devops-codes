WITH base_logs AS (
  SELECT
    cs_uri_stem,
    event_minutes
  FROM cloudfront.me_cf
  WHERE
    cloud_front_id = 'EVIQ7AQNYX0D'
    AND event_year = 2025
    AND event_month = 7
    AND event_day = 17
    AND event_hour BETWEEN 0 AND 23
    AND event_minutes BETWEEN 0 AND 59
),
normalized_logs AS (
  SELECT
    event_minutes,
    
    -- Apply patterns in sequence (most specific first)
    CASE
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/remove-coupon/') THEN '/v1/carts/:cartId/remove-coupon/'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/apply-coupon/[^/]+') THEN '/v1/carts/:cartId/apply-coupon/:couponCode'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/apply-wallet/[^/]+') THEN '/v1/carts/:cartId/apply-wallet/:userId'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/[^/]+/items') THEN '/v1/carts/:type/:cartId/items'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/getNewCart') THEN '/v1/carts/:userId/getNewCart'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/clearCouponCache/[^/]+') THEN '/v1/carts/clearCouponCache/:ruleId'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/coupon-journey/[^/]+') THEN '/v1/carts/coupon-journey/:type'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/coupon-list/[^/]+/[^/]+') THEN '/v1/carts/coupon-list/:type/:cartId'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/getNpsByOrder/[^/]+/[^/]+') THEN '/v1/carts/getNpsByOrder/:incrementId/:platform'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/getNpsConfig/[^/]+/type/[^/]+') THEN '/v1/carts/getNpsConfig/:platform/type/:npsType'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/pincode/[^/]+') THEN '/v1/carts/pincode/:postcode'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/add-bulk-products') THEN '/v1/carts/:cartId/add-bulk-products'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/remove-wallet/[^/]+') THEN '/v1/carts/:cartId/remove-wallet/:userId'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/[^/]+/add-free-products') THEN '/v1/carts/:type/:cartId/add-free-products'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/[^/]+/addFreeGifts') THEN '/v1/carts/:type/:cartId/addFreeGifts'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/[^/]+/address-information') THEN '/v1/carts/:type/:cartId/address-information'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/[^/]+/[^/]+/totals') THEN '/v1/carts/:type/:cartId/totals'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/applyGiftCard/[^/]+') THEN '/v1/carts/applyGiftCard/:type'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/applyGiftCardGyftr/[^/]+') THEN '/v1/carts/applyGiftCardGyftr/:type'
      WHEN REGEXP_LIKE(cs_uri_stem, '^/v1/carts/removeGiftCard/[^/]+') THEN '/v1/carts/removeGiftCard/:type'
      ELSE cs_uri_stem
    END AS normalized_uri

  FROM base_logs
)
SELECT
  COUNT(*) AS request_count,
  normalized_uri,
  event_minutes
FROM normalized_logs
GROUP BY normalized_uri, event_minutes
ORDER BY event_minutes, request_count DESC;