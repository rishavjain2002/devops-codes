patterns = [
    (r'/v1/carts/(.+)/remove-coupon/', '/v1/carts/:cartId/remove-coupon/'),
    (r'/v1/carts/(.+)/apply-coupon/(.+)', '/v1/carts/:cartId/apply-coupon/:couponCode'),
    (r'/v1/carts/(.+)/apply-wallet/(.+)', '/v1/carts/:cartId/apply-wallet/:userId'),
    (r'/v1/carts/(.+)/(.+)/items', '/v1/carts/:type/:cartId/items'),
    (r'/v1/carts/(.+)/coupon-eligible', '/v1/carts/:type/coupon-eligible'),
    (r'/v1/carts/(.+)/getNewCart', '/v1/carts/:userId/getNewCart'),
    (r'/v1/carts/coupon-list/(.+)/(.+)', '/v1/carts/coupon-list/:type/:cartId'),

    (r'/v1/carts/getNpsByOrder/(.+)/(.+)', '/v1/carts/getNpsByOrder/:incrementId/:platform'),
    (r'/v1/carts/getNpsConfig/(.+)/type/(.+)', '/v1/carts/getNpsConfig/:platform/type/:npsType'),
    (r'/v1/carts/getProductActivePurchase/(.+)', '/v1/carts/getProductActivePurchase/:sku'),
    (r'/v1/carts/pincode/(.+)', '/v1/carts/pincode/:postcode'),

    (r'/v1/carts/(.+)/remove-wallet/(.+)', '/v1/carts/:cartId/remove-wallet/:userId'),
    (r'/v1/carts/(.+)/(.+)/add-free-products', '/v1/carts/:type/:cartId/add-free-products'),
    (r'/v1/carts/(.+)/(.+)/address-information', '/v1/carts/:type/:cartId/address-information'),
    (r'/v1/carts/(.+)/(.+)/multiAddToCart', '/v1/carts/:type/:cartId/multiAddToCart'),
    (r'/v1/carts/(.+)/(.+)/totals', '/v1/carts/:type/:cartId/totals'),
    (r'/v1/carts/(.+)?/(.+)?/getProductsActivePurchase', '/v1/carts/:type?/:cartId?/getProductsActivePurchase'),
    (r'/v1/carts/add', '/v1/carts/add'),
    (r'/v1/carts/samples/add/(.+)/(.+)', '/v1/carts/samples/add/:type/:phone'),
]