#!/usr/bin/env python
# -*- coding: utf-8 -*-
from saml2 import BINDING_HTTP_POST
from saml2.saml import NAMEID_FORMAT_UNSPECIFIED

try:
    from saml2.sigver import get_xmlsec_binary
except ImportError:
    get_xmlsec_binary = None

if get_xmlsec_binary:
    xmlsec_path = get_xmlsec_binary(["/opt/local/bin"])
else:
    xmlsec_path = "/usr/local/bin/xmlsec1"


# HTTPS cert information
CERT_FILE = "/Users/malagusundaram/Downloads/EnvSamlUAT.pem"
# SIGN_ALG = ds.SIG_RSA_SHA512
# DIGEST_ALG = ds.DIGEST_SHA512
CONFIG = {
    "entityid": "eotest.xxxxxx.com",
    "valid_for": 168,
    "service": {
        "idp": {
            "name": "TEST IDP",
            "endpoints": {
                "single_sign_on_service": [
                    (
                        "https://uat.xxxxxx.com/openenv/api/auth/login?firm=xxxxxx",
                        BINDING_HTTP_POST,
                    ),
                ],
            },
            "name_id_format": [NAMEID_FORMAT_UNSPECIFIED],
        },
    },
    "key_file": CERT_FILE,
    "cert_file": CERT_FILE,
    "xmlsec_binary": xmlsec_path,
}
