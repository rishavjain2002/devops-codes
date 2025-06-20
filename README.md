# Copy CloudFront Distribution
## Using Using AWS cli


```
aws cloudfront list-distributions  
```

```
aws cloudfront get-distribution --id "ETQM52ZK5JURL"
```


```
aws cloudfront get-distribution-config --id "ETQM52ZK5JURL" --profile=ris | jq '.DistributionConfig' > dist-config.json

```

🛠 Summary Checklist Before create-distribution

✅ Update "CallerReference" with a unique value \
✅ Update or remove "Aliases" \
✅ Set "Staging": true \
✅ Update "ViewerCertificate" if using different domains/certs \
✅ Ensure "TargetOriginId" matches any updated "Origins" \
✅ Remove fields: "ETag", "Id", "Status", "LastModifiedTime" 



```
aws cloudfront create-distribution --distribution-config file://~/Desktop/dist-config.json
```

```
aws cloudfront get-distribution-config --id "$NEW_DIST_ID" > new-dist-config.json
```

Set Etag
```
STAGING_ETAG=$(aws cloudfront get-distribution-config --id "$NEW_DIST_ID" --query 'ETag' --output text)
```

Edit new-dist-config.json to add aliases
```
aws cloudfront update-distribution \
  --id "$NEW_DIST_ID" \
  --if-match "$STAGING_ETAG" \
  --distribution-config file://new-dist-config.json
```













