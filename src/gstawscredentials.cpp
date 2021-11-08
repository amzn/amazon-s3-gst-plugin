/* amazon-s3-gst-plugin
 * Copyright (C) 2019 Marcin Kolny <marcin.kolny@gmail.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */
#include "gstawscredentials.hpp"

#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/sts/model/AssumeRoleRequest.h>
#include <aws/sts/STSClient.h>

GST_DEBUG_CATEGORY_STATIC (gst_aws_credentials_debug);
#define GST_CAT_DEFAULT gst_aws_credentials_debug

using namespace Aws::Auth;

struct _GstAWSCredentials {
  _GstAWSCredentials(GstAWSCredentialsProviderFactory factory) :
    credentials_provider_factory(std::move(factory))
  {
  }

  GstAWSCredentialsProviderFactory credentials_provider_factory;
};

GstAWSCredentials *
gst_aws_credentials_new (GstAWSCredentialsProviderFactory factory)
{
  return new GstAWSCredentials(std::move(factory));
}

GstAWSCredentials *
gst_aws_credentials_new_default (void)
{
  return gst_aws_credentials_new ([] {
    return std::unique_ptr<AWSCredentialsProvider> (new DefaultAWSCredentialsProviderChain ());
  });
}

std::unique_ptr<AWSCredentialsProvider>
gst_aws_credentials_create_provider (GstAWSCredentials * credentials)
{
  return credentials->credentials_provider_factory();
}

GstAWSCredentials *
gst_aws_credentials_copy (GstAWSCredentials * credentials)
{
  return gst_aws_credentials_new (credentials->credentials_provider_factory);
}

void
gst_aws_credentials_free (GstAWSCredentials * credentials)
{
  delete credentials;
}

static bool
strings_equal(const gchar* str1, const gchar* str2, size_t len2)
{
	return strlen(str1) == len2 && strncmp(str1, str2, len2) == 0;
}

static bool
is_null_or_empty(const char* str)
{
  return str == NULL || strcmp(str, "") == 0;
}

static std::unique_ptr<AWSCredentialsProvider>
_gst_aws_credentials_assume_role(const gchar * role_arn, std::shared_ptr<AWSCredentialsProvider> base_provider)
{
  Aws::STS::Model::AssumeRoleOutcome response = Aws::STS::STSClient(base_provider)
      .AssumeRole(Aws::STS::Model::AssumeRoleRequest().WithRoleArn(role_arn)
      // Use access key of the currently used AWS account as a session name
      .WithRoleSessionName(base_provider->GetAWSCredentials().GetAWSAccessKeyId()));

  if (!response.IsSuccess())
  {
    return NULL;
  }

  Aws::STS::Model::Credentials role_credentials = response.GetResult().GetCredentials();

  return std::unique_ptr<AWSCredentialsProvider> (
    new SimpleAWSCredentialsProvider (
      AWSCredentials(role_credentials.GetAccessKeyId(),
        role_credentials.GetSecretAccessKey(),
        role_credentials.GetSessionToken())));
 }

static std::unique_ptr<AWSCredentialsProvider>
_gst_aws_credentials_create_provider(const gchar * access_key_id, const gchar * secret_access_key, const gchar * session_token)
{
  if (is_null_or_empty(access_key_id) || is_null_or_empty(secret_access_key))
  {
    if (!is_null_or_empty(session_token)) {
      GST_ERROR ("access-key-id and secret-access-key must be set to use session token");
      return NULL;
    }
    if (!is_null_or_empty(access_key_id) || !is_null_or_empty(secret_access_key)) {
      GST_ERROR ("Either both access-key-id and secret-access-key must be set or none of them.");
      return NULL;
    }
    return std::unique_ptr<AWSCredentialsProvider> (new DefaultAWSCredentialsProviderChain());
  }
  else
  {
    return std::unique_ptr<AWSCredentialsProvider> (
      new SimpleAWSCredentialsProvider (AWSCredentials(access_key_id, secret_access_key, session_token ? session_token : "")));
  }
}

static std::unique_ptr<AWSCredentialsProvider>
_gst_aws_credentials_provider_from_string(const gchar * str)
{
  gchar **parameters = g_strsplit (str, "|", -1);
  gchar **param = parameters;

  const gchar *access_key_id = NULL;
  const gchar *secret_access_key = NULL;
  const gchar *iam_role = NULL;
  const gchar *session_token = NULL;

  while (*param) {
    const gchar *value = g_strstr_len (*param, -1, "=");
    if (!value) {
      GST_WARNING ("Expected format property 'param=value', was: '%s'", *param);
    } else {
      size_t len = value - *param;
      value++;
      if (strings_equal ("access-key-id", *param, len)) {
        access_key_id = value;
      } else if (strings_equal ("secret-access-key", *param, len)) {
        secret_access_key = value;
      } else if (strings_equal ("iam-role", *param, len)) {
        iam_role = value;
      } else if (strings_equal ("session-token", *param, len)) {
        session_token = value;
      } else {
        GST_WARNING ("Unknown parameter '%.*s'", (int)len, *param);
      }
    }
    param++;
  }

  auto provider = _gst_aws_credentials_create_provider(access_key_id, secret_access_key, session_token);

  if (!provider) {
    GST_ERROR ("Failed to create AWS credentials provider");
    return NULL;
  }

  if (!is_null_or_empty (iam_role))
    provider = _gst_aws_credentials_assume_role(iam_role, std::move(provider));

  g_strfreev (parameters);

  return provider;
}

static GstAWSCredentials *
_gst_aws_credentials_from_string (const gchar * str)
{
  std::string credentials_str = str;
  return gst_aws_credentials_new ([credentials_str] {
    return _gst_aws_credentials_provider_from_string (credentials_str.c_str());
  });
}

static gboolean
_gst_aws_credentials_deserialize (GValue * value, const gchar * s)
{
  GstAWSCredentials *credentials;

  credentials = _gst_aws_credentials_from_string (s);

  if (credentials) {
    g_value_take_boxed (value, credentials);
    return TRUE;
  }

  return FALSE;
}

static void
string_to_aws_credentials (const GValue * src_value, GValue * dest_value)
{
  const gchar *credentials_str = g_value_get_string (src_value);
  _gst_aws_credentials_deserialize (dest_value, credentials_str);
}

static gboolean
_gst_aws_credentials_deserialize_valfunc (GValue * value, const gchar * s)
{
  return _gst_aws_credentials_deserialize (value, s);
}

static void
_do_init (GType g_define_type_id)
{
  g_value_register_transform_func (G_TYPE_STRING, g_define_type_id, string_to_aws_credentials);

  static GstValueTable gstvtable = {
    g_define_type_id,
    (GstValueCompareFunc) NULL,
    (GstValueSerializeFunc) NULL,
    (GstValueDeserializeFunc) _gst_aws_credentials_deserialize_valfunc,
    { NULL }
  };

  gst_value_register (&gstvtable);

 GST_DEBUG_CATEGORY_INIT (gst_aws_credentials_debug, "aws-credentials", 0, "AWS credentials");
}

G_DEFINE_BOXED_TYPE_WITH_CODE(GstAWSCredentials, gst_aws_credentials,
       gst_aws_credentials_copy,
       gst_aws_credentials_free,
       _do_init (g_define_type_id))
