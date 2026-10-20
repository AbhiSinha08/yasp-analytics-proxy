# Local decision model

This setup runs Laya as a local, Jev-compatible decision API for development and
demos. It does not change YASP. The model and its weights run in Docker; only the
first start needs internet access to fetch the pinned public checkpoint.

## Model and resource settings

The service builds the latest Laya source from its `main` branch and uses its
`typed-decisions` checkpoint. That checkpoint has 421 million parameters and a
1,024-token default context. One checkpoint is resident at a time. The API
admits up to four requests and allows explicit per-request token limits up to
2,048; Laya serializes model inference through one worker, so the concurrency
setting prevents early busy responses but does not run four GPU inferences in
parallel. The CPU configuration uses four threads and an 8 GiB container memory
limit. The upstream CPU setup calls for 8 GiB RAM and about 10 GiB free disk;
Docker keeps the downloaded model in a named volume.

The Laya source revision follows the current `main` branch tip when the image is
built. `LAYA_REVISION=reviewed` keeps the model checkpoint at its reviewed SHA
(`55cf4c4ebb4ebe31b2550e8bdf3bd21b99753851`) between rebuilds. Rebuild the image
with `--build` to pick up newer Laya source.

The CUDA compose override uses PyTorch 2.14.0 with CUDA 13.0. By default it
keeps the checkpoint's precision setting instead of forcing fp16. Set
`LAYA_CUDA_AMP=fp16` only if you want to trade precision for lower GPU memory
use. The typed-decisions checkpoint defaults to a 1,024-token context, so the
larger server limit only permits explicit request overrides; it does not
increase the default context or make short requests more accurate.

## Start

Run from the repository root. For an NVIDIA GPU, start with:

```sh
docker compose \
  -f dev/decision-model/compose.yml \
  -f dev/decision-model/compose.cuda.yml \
  up -d --build --wait
```

If Docker cannot access the GPU, or to use CPU inference:

```sh
docker compose -f dev/decision-model/compose.yml up -d --build --wait
```

The endpoint is bound to loopback at `http://127.0.0.1:8000`. Check it with:

```sh
curl --fail http://127.0.0.1:8000/health
```

To see which checkpoint revision and device actually loaded, the health response
includes `loaded`, `revisions`, and `device`. A healthy status alone only checks
that the HTTP process is up. With the CUDA compose file, verify that `device` is
`cuda`; with CPU mode it is `cpu`.

## Send a PII-column decision

The example contains five invented rows, the SQL that produced them, and one
yes/no (`noul`) question for each PII category. The model returns a probability
for each question under `answers.<question-id>.noul`.

```sh
curl --fail-with-body http://127.0.0.1:8000/v1/systemone \
  -H 'Content-Type: application/json' \
  --data @dev/decision-model/examples/pii-columns.json
```

For YASP, the local base URL is `http://127.0.0.1:8000` and the decision route
is `POST /v1/systemone`. Send the SQL, the bounded sample, and the candidate
PII questions in the request `state` and `questions` fields. Use one `noul`
question per column and data entity, such as PAN, date of birth, address, or
postcode; this allows a column to match more than one category. The response's
probabilities are model predictions, not access-control decisions. YASP does
not call this service yet.

The service uses the Jev decision request shape; it does not expose an
OpenAI-style `/v1/chat/completions` endpoint. It binds to loopback and has no API
key by default. If you set `LAYA_API_KEY` in the environment before `docker
compose up`, send the matching `Authorization: Bearer ...` header. Do not expose
the unauthenticated service on a network interface.

Laya is not trained specifically to find PII. Its project reports weak
zero-shot results on its general typed-decision benchmark, so evaluate it on
representative labeled YASP samples before using the scores to make masking or
access decisions. On the synthetic example, the returned probabilities were
0.58–0.62; a successful API response proves connectivity and output shape, not
reliable PII detection.

## Stop

Use the same compose files used to start it:

```sh
docker compose -f dev/decision-model/compose.yml -f dev/decision-model/compose.cuda.yml down
```

For CPU mode, omit `-f dev/decision-model/compose.cuda.yml`. The named model
cache is kept by `down`; add `--volumes` to download the checkpoint again next
time.
