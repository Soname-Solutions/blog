#!/usr/bin/env bash

if [[ $1 == "" ]]; then
  echo "Please, provide S3 Bucket Name as a parameter"
  exit 1
fi

script_folder=$(dirname "${BASH_SOURCE[0]}")

s3_bucket_name=$1
profile_name=$2

# delete all versions
aws s3api list-object-versions --bucket ${s3_bucket_name} --query='{Objects: Versions[].{Key:Key,VersionId:VersionId}}' > ${script_folder}/tmp.json 
aws s3api delete-objects --bucket ${s3_bucket_name} --delete file://${script_folder}/tmp.json 

# delete all "delete markers" for objects
aws s3api list-object-versions --bucket ${s3_bucket_name} --query='{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' > ${script_folder}/tmp.json 
aws s3api delete-objects --bucket ${s3_bucket_name} --delete file://${script_folder}/tmp.json 

rm ${script_folder}/tmp.json