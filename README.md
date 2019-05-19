[![Build Status](https://travis-ci.org/amzn/amazon-s3-gst-plugin.svg?branch=master)](https://travis-ci.org/amzn/amazon-s3-gst-plugin)

## Amazon S3 Gst Plugin

A collection of [GStreamer](https://gstreamer.freedesktop.org/) elements to interact
with the [Amazon Simple Storage Service (S3)](https://aws.amazon.com/s3/).

## Elements
* s3sink - streams the multimedia to a specified bucket.

## AWS Credentials
By default all the elements use the [default credentials provider chain](https://sdk.amazonaws.com/cpp/api/0.14.3/class_aws_1_1_auth_1_1_default_a_w_s_credentials_provider_chain.html), which means, that credentials are read from the following sources:

1. Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
1. AWS credentials file. Usually located at ~/.aws/credentials.
1. For EC2 instance, IAM instance profile.

Some of the elements have `credentials` property of `GstAWSCredentials` type, which is a wrapper for an `Aws::Auth::AWSCredentialsProvider` class.

### Defining GstAWSCredentials as a string
The `GstAWSCredentials` object can be deserialized from a string, which makes using the property in gst-launch command possible. The string must be specified in the following format
```
property1=value1[|property2=value2[|property3=value3[|...]]]
```
Currently the deserializer accepts following properties:

* `access-key-id`, e.g. `AKIAIOSFODNN7EXAMPLE`
* `secret-access-key`, e.g. `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
* `session-token`
* `iam-role`, e.g. `arn:aws:iam::123456789012:role/s3access`

All the properties are optional, however, be aware of the rules:

* `access-key-id` cannot exist without `secret-access-key` (and vice versa)
* if `session-token` specified, both `access-key-id` and `secret-access-key` must be present
* if `iam-role` is specified, it will use default credentials provider to assume the role, unless `access-key-id` and `secret-access-key` are present - in that case, these credentials are used to assume the role.

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