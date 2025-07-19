patterns = [
    (r'/v1/customer/(.+)/address$', '/v1/customer/:userId/address'),
    (r'/v1/customer/(.+)/address/default$', '/v1/customer/:userId/address/default'),
    (r'/v1/customer/(.+)/balance-history$', '/v1/customer/:userId/balance-history'),
    (r'/v1/customer/(.+)/cart$', '/v1/customer/:userId/cart'),
    (r'/v1/customer/(.+)/create-quote$', '/v1/customer/:userId/create-quote'),
    (r'/v1/customer/(.+)/info$', '/v1/customer/:userId/info'),
    (r'/v1/customer/(.+)/order/(.+)/invoice$', '/v1/customer/:userId/order/:orderId/invoice'),
    (r'/v1/customer/(.+)/orders$', '/v1/customer/:userId/orders'),
    (r'/v1/customer/(.+)/orders/(.+)$', '/v1/customer/:userId/orders/:orderId'),
    (r'/v1/customer/(.+)/profile$', '/v1/customer/:userId/profile'),

    (r'/v1/customer/fetchMamacash/(.+)$', '/v1/customer/fetchMamacash/:phone'),

    (r'/v1/customer/order/(.+)/(.+)/cashback$', '/v1/customer/order/:orderId/:email/cashback'),

    (r'/v1/customer/(.+)/customer-trees$', '/v1/customer/:type/customer-trees'),
    (r'/v1/customer/(.+)/is-repeated-user$', '/v1/customer/:type/is-repeated-user'),
    (r'/v1/customer/(.+)/repeat-user$', '/v1/customer/:type/repeat-user'),
    (r'/v1/customer/orders/(.+)$', '/v1/customer/orders/:phone'),

    (r'/v1/customer/order/(.+)/feedback$', '/v1/customer/order/:orderId/feedback'),
    (r'/v1/customer/(.+)/address$', '/v1/customer/:userId/address'),  # For POST/PUT/DELETE
    (r'/v1/customer/(.+)/profile$', '/v1/customer/:userId/profile'),  # For PUT
    
]