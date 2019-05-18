[![Build Status](https://travis-ci.org/amzn/amazon-s3-gst-plugin.svg?branch=master)](https://travis-ci.org/amzn/amazon-s3-gst-plugin)

## Amazon S3 Gst Plugin

A collection of [GStreamer](https://gstreamer.freedesktop.org/) elements to interact
with the [Amazon Simple Storage Service (S3)](https://aws.amazon.com/s3/).

## Elements
* s3sink - streams the multimedia to a specified bucket.

## AWS Credentials
All the elements use the [default credentials provider chain](https://sdk.amazonaws.com/cpp/api/0.14.3/class_aws_1_1_auth_1_1_default_a_w_s_credentials_provider_chain.html), that is:

1. Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
1. AWS credentials file. Usually located at ~/.aws/credentials.
1. For EC2 instance, IAM instance profile.

Moreover, some of the elements have `iam-role` property - the element then uses the credentials to assume the specified IAM role.

## License Summary
This code is made available under the LGPLv2.1 license. 
(See [LICENSE](LICENSE) file)

## Examples
* Streaming camera capture to an S3 bucket:
```
$ gst-launch-1.0 -e v4l2src num-buffers=300 device=/dev/video0 ! x264enc ! matroskamux ! s3sink bucket=my-personal-videos key=recording.mkv
```

## Contributing
Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.