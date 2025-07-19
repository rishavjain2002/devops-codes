import re
import pandas as pd
def lambda_handler(uri):
    patterns = [
        (r'/v1/order/fetchOrderShipments/(.+)', '/v1/order/fetchOrderShipments/:orderId'),
        (r'/v1/order/getCustomerDetails/(.+)', '/v1/order/getCustomerDetails/:orderId'),
        (r'/v1/order/orderInvoicePdf/(.+)/(.+)', '/v1/order/orderInvoicePdf/:userId/:orderId'),

        (r'/v1/order/(.+)/create-order', '/v1/order/:type/create-order'),
        (r'/v1/order/(.+)/deleteRzpToken/(.+)/(.+)', '/v1/order/:type/deleteRzpToken/:customerId/:token'),
        (r'/v1/order/(.+)/fetchRzpTokens', '/v1/order/:type/fetchRzpTokens'),
       
        (r'/v1/order/verify-juspay-payment/(.+)', '/v1/order/verify-juspay-payment/:type'),
    ]

    for pattern, replacement in patterns:
        if re.search(pattern, uri):
            uri = re.sub(pattern, replacement, uri)
        
    return uri


ans = lambda_handler('/v1/catalog/products/id/89/reviewsWithPagination')
print(ans)