from google.cloud import secretmanager
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/artem_iakovenko/service-account/secret-manager.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tokens/secret-manager.json"
client = secretmanager.SecretManagerServiceClient()


def access_secret(project_id: str, secret_id: str, version_id: str = "latest"):
  name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
  response = client.access_secret_version(request={"name": name})
  secret_value = response.payload.data.decode("UTF-8")
  return secret_value
