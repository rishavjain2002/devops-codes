import re
import pandas as pd
def lambda_handler(uri):
    patterns = [
        (r'/v1/carts/(.+)/remove-coupon/', '/v1/carts/:cartId/remove-coupon/'),
        (r'/v1/carts/(.+)/apply-coupon/(.+)', '/v1/carts/:cartId/apply-coupon/:couponCode'),
        (r'/v1/carts/(.+)/apply-wallet/(.+)', '/v1/carts/:cartId/apply-wallet/:userId'),
        (r'/v1/carts/(.+)/(.+)/items', '/v1/carts/:type/:cartId/items'),
        (r'/v1/carts/(.+)/getNewCart', '/v1/carts/:userId/getNewCart'),
        (r'/v1/carts/clearCouponCache/(.+)', '/v1/carts/clearCouponCache/:ruleId'),
        (r'/v1/carts/coupon-journey/(.+)', '/v1/carts/coupon-journey/:type'),
        (r'/v1/carts/coupon-list/(.+)/(.+)', '/v1/carts/coupon-list/:type/:cartId'),
        (r'/v1/carts/getNpsByOrder/(.+)/(.+)', '/v1/carts/getNpsByOrder/:incrementId/:platform'),
        (r'/v1/carts/getNpsConfig/(.+)/type/(.+)', '/v1/carts/getNpsConfig/:platform/type/:npsType'),
        (r'/v1/carts/pincode/(.+)', '/v1/carts/pincode/:postcode'),
        (r'/v1/carts/(.+)/add-bulk-products', '/v1/carts/:cartId/add-bulk-products'),
        (r'/v1/carts/(.+)/remove-wallet/(.+)', '/v1/carts/:cartId/remove-wallet/:userId'),
        (r'/v1/carts/(.+)/(.+)/add-free-products', '/v1/carts/:type/:cartId/add-free-products'),
        (r'/v1/carts/(.+)/(.+)/addFreeGifts', '/v1/carts/:type/:cartId/addFreeGifts'),
        (r'/v1/carts/(.+)/(.+)/address-information', '/v1/carts/:type/:cartId/address-information'),
        (r'/v1/carts/(.+)/(.+)/totals', '/v1/carts/:type/:cartId/totals'),
        (r'/v1/carts/applyGiftCard/(.+)', '/v1/carts/applyGiftCard/:type'),
        (r'/v1/carts/applyGiftCardGyftr/(.+)', '/v1/carts/applyGiftCardGyftr/:type'),
        (r'/v1/carts/removeGiftCard/(.+)', '/v1/carts/removeGiftCard/:type'),
    ]



    
# ...existing code...

    for pattern, replacement in patterns:
        if re.search(pattern, uri):
            uri = re.sub(pattern, replacement, uri)
        
    return uri


ans = lambda_handler('/v1/catalog/products/id/89/reviewsWithPagination')
print(ans)