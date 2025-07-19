import re
import pandas as pd
def lambda_handler(uri):
    patterns = [
        (r'/v1/auth/email-available/(.*)', '/v1/auth/email-available/:email')
    ]



    for pattern, replacement in patterns:
        if re.search(pattern, uri):
            uri = re.sub(pattern, replacement, uri)
        
    return uri


ans = lambda_handler('/v1/catalog/products/id/89/reviewsWithPagination')
print(ans)