"""Azure Key Vault secret retrieval helpers.

.. warning::
    This module's filename (``secrets.py``) shadows the Python standard
    library :mod:`secrets` module. The plan dictates this path
    (``on-prem-to-fabric-wh/src/secrets.py``), so the file cannot be renamed.
    Future readers / importers must avoid the ambiguity by importing this
    module via its fully-qualified package path and aliasing it, e.g.::

        from on-prem-to-fabric-wh.src import secrets as kv_secrets

    or by referencing it directly by file path. Do **not** ``import secrets``
    from inside this package expecting to get the stdlib module.

This module is intentionally decoupled from ``config.py`` so it can be reused
in any context where a Key Vault name + secret name are already known.
"""

from __future__ import annotations

import logging

from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceNotFoundError,
)
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

logger = logging.getLogger(__name__)


def get_spn_secret(kv_name: str, secret_name: str) -> str:
    """Fetch a secret value from an Azure Key Vault.

    Uses :class:`azure.identity.DefaultAzureCredential` to authenticate and
    :class:`azure.keyvault.secrets.SecretClient` to read the secret. The
    secret value itself is never logged.

    Args:
        kv_name: The short name of the Key Vault (the part before
            ``.vault.azure.net``).
        secret_name: The name of the secret to retrieve.

    Returns:
        The secret's current value as a string.

    Raises:
        PermissionError: The caller's identity is not authorized to read
            the secret (HTTP 403). The message instructs the user to grant
            the ``Key Vault Secrets User`` role to their identity.
        LookupError: The named secret does not exist in the vault
            (HTTP 404).
        RuntimeError: Authentication failed before the request was made
            (e.g. no credentials available). The message points the user
            to ``az login``.
        HttpResponseError: Any other unexpected HTTP error from Key Vault
            is re-raised unchanged.
    """
    vault_url = f"https://{kv_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)

    try:
        secret = client.get_secret(secret_name)
    except ResourceNotFoundError as exc:
        raise LookupError(
            f"Secret '{secret_name}' was not found in Key Vault '{kv_name}'."
        ) from exc
    except ClientAuthenticationError as exc:
        raise RuntimeError(
            "Failed to authenticate to Azure while fetching secret "
            f"'{secret_name}' from Key Vault '{kv_name}'. "
            "Run 'az login' (or configure a managed identity / service "
            "principal) and try again."
        ) from exc
    except HttpResponseError as exc:
        status = getattr(exc, "status_code", None)
        if status == 403:
            raise PermissionError(
                f"Access denied reading secret '{secret_name}' from Key "
                f"Vault '{kv_name}'. Grant the 'Key Vault Secrets User' "
                "role to your identity on this vault and try again."
            ) from exc
        if status == 404:
            raise LookupError(
                f"Secret '{secret_name}' was not found in Key Vault "
                f"'{kv_name}'."
            ) from exc
        raise

    logger.info("fetched secret '%s' from vault '%s'", secret_name, kv_name)
    return secret.value
