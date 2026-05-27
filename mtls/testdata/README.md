# mtls testdata

`client-cert.p12` is a throwaway self-signed RSA-2048 client certificate
used only by the mtls package's unit tests. Subject CN: `yggdrasil-sdk-test`.
No password. Valid for 3650 days from generation.

`client-cert-pwd.p12` is the same key+cert wrapped with password `secret`.

To regenerate (e.g. after expiry):

```bash
openssl req -x509 -newkey rsa:2048 -keyout /tmp/k.pem -out /tmp/c.pem \
  -days 3650 -nodes -subj "/CN=yggdrasil-sdk-test"
openssl pkcs12 -export -out mtls/testdata/client-cert.p12 \
  -inkey /tmp/k.pem -in /tmp/c.pem -passout pass:
openssl pkcs12 -export -out mtls/testdata/client-cert-pwd.p12 \
  -inkey /tmp/k.pem -in /tmp/c.pem -passout pass:secret
rm /tmp/k.pem /tmp/c.pem
```

These files contain **no production keys** and may be committed.
