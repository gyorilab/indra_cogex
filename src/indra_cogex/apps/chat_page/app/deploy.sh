#!/bin/bash

S3_URI_BASE=s3://bigmech/
PATH_DEFAULT=chat

helpFunction()
{
   echo ""
   echo "Usage: $0 [-i INDRALAB_VUE_TGZ] [-s S3_PATH]"
   echo -e "\t-i Path to indralab-vue tgz file. If provided, it will be installed. If not, it is assumed to already be installed."
   echo -e "\t-s The S3 bucket path to deploy to. Default is $S3_URI_BASE$PATH_DEFAULT"
   exit 1 # Exit script after printing help
}

while getopts "is" opt
do
   case "$opt" in
      i ) indralabVueTgz="$OPTARG" ;;
      s ) s3Path="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty (not needed for optional parameters)
#if [ -z "$indralabVueTgz" ] || [ -z "$s3Path" ]
#then
#   echo "Some or all of the parameters are empty";
#   helpFunction
#fi

# Install indralab-vue if provided
if [ -n "$indralabVueTgz" ]
then
   echo "Installing indralab-vue from ${indralabVueTgz}"
   npm install "${indralabVueTgz}"
fi

# Build the app
echo "Building the app"
npm run build

# Deploy to S3
if [ "$s3Path" ]; then
    PATH=$1
else # Use the alternate S3 URI
    PATH=$PATH_DEFAULT
fi

S3_URI="${S3_URI_BASE}${PATH}"
echo "Deploying to $PATH"

# Copy the content of the dist directory to the S3 bucket
# See https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/sync.html for more details
aws s3 sync --exact-timestamps --delete dist/ "${S3_URI}"

# Force a cache refresh for CloudFront. NOTE: This requires SUDO privileges currently and will likely fail
#aws cloudfront create-invalidation --distribution-id EFROMZ1D89URP --paths "/chat*"
echo "Invalidate cache manually by going to https://us-east-1.console.aws.amazon.com/cloudfront/v3/home?region=us-east-1#/distributions"
echo "  - Click the distribution for discovery.indra.bio"
echo "  - Click the invalidations tab"
echo "  - Click 'Create invalidation'"
echo "  - Add the path /chat* to the list of paths to be invalidated"
echo "  - Click 'Create invalidation'"
echo "  - Wait for the invalidation to complete"
