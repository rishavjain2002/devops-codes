import re
import pandas as pd
def lambda_handler(uri):
    patterns = [
        (r'/v1/customer/(.+)/address', '/v1/customer/:userId/address'),
        (r'/v1/customer/(.+)/address/default', '/v1/customer/:userId/address/default'),
        (r'/v1/customer/(.+)/balance-history', '/v1/customer/:userId/balance-history'),
        (r'/v1/customer/(.+)/cart$', '/v1/customer/:userId/cart'),
        (r'/v1/customer/(.+)/create-quote$', '/v1/customer/:userId/create-quote'),
        (r'/v1/customer/(.+)/info$', '/v1/customer/:userId/info'),
        (r'/v1/customer/(.+)/order/(.+)/invoice', '/v1/customer/:userId/order/:orderId/invoice'),
        (r'/v1/customer/(.+)/order/ids$', '/v1/customer/:userId/order/ids'),
        (r'/v1/customer/(.+)/orders$', '/v1/customer/:userId/orders'),
        (r'/v1/customer/(.+)/orders/(.+)$', '/v1/customer/:userId/orders/:orderId'),
        (r'/v1/customer/(.+)/profile$', '/v1/customer/:userId/profile'),
        (r'/v1/customer/fetchMamacash/(.+)$', '/v1/customer/fetchMamacash/:phone'),
        (r'/v1/customer/order/(.+)/(.+)/cashback$', '/v1/customer/order/:orderId/:email/cashback'),
        (r'/v1/customer/order/details/(.+)$', '/v1/customer/order/details/:entityId'),
        (r'/v1/customer/sankalptaru-tree/(.+)$', '/v1/customer/sankalptaru-tree/:orderId'),
        (r'^/v1/customer/(.+)/info$', '/v1/customer/:userId/info'),
        (r'/v1/customer/(.+)/customer-trees$', '/v1/customer/:type/customer-trees'),
        (r'/v1/customer/(.+)/is-repeated-user$', '/v1/customer/:type/is-repeated-user'),
        (r'/v1/customer/(.+)/recommendations$', '/v1/customer/:type/recommendations'),
        (r'/v1/customer/(.+)/repeat-user$', '/v1/customer/:type/repeat-user'),
        (r'/v1/customer/orders/(.+)$', '/v1/customer/orders/:phone'),
    ]



    
    for pattern, replacement in patterns:
        if re.search(pattern, uri):
            uri = re.sub(pattern, replacement, uri)
        
    return uri


ans = lambda_handler('/v1/catalog/products/id/89/reviewsWithPagination')
print(ans)