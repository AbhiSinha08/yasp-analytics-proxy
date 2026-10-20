# Local decision model

Run the Laya decision API with Python from the repository root. Python uses the
latest Laya `main` branch; the first start downloads the model.

## Install and run

For CPU, install PyTorch with the first command below. For GPU, use the matching
PyTorch command from the [install selector](https://pytorch.org/get-started/locally/)
in its place. Then install Laya and start the server:

```sh
python -m pip install torch
python -m pip install "laya[serve] @ git+https://github.com/NandhaKishorM/laya.git@main"
python dev/decision-model/serve.py
```

The API listens on `http://127.0.0.1:8001`. Check the loaded device with
`GET /health`; send decisions to `POST /v1/systemone`.

```sh
curl --fail http://127.0.0.1:8001/health
curl --fail-with-body http://127.0.0.1:8001/v1/systemone \
  -H 'Content-Type: application/json' \
  --data-binary @dev/decision-model/examples/pii-columns.json
```

`LAYA_DEVICE=auto` is the default and uses a supported accelerator when the
installed PyTorch build exposes one. Set `LAYA_DEVICE=cpu` to force CPU or
`LAYA_REQUIRE_GPU=1` to fail if the model falls back to CPU. `LAYA_LOG_LEVEL`
defaults to `debug`; `LAYA_MAX_CONCURRENT` defaults to `8` and limits admitted
requests. Model inference itself runs one request at a time. `PORT` changes the
default port `8001`. `LAYA_THREADS` defaults to `4`; `LAYA_MAX_TOKEN_BUDGET`
defaults to `2048` and caps an optional request `max_len`.

The response includes each question's `noul` probability and `confidence`, plus
token/truncation information. `noul` is the predicted probability that a column
contains the named PII; `confidence` describes certainty in the more likely
answer and can therefore be high for “no”. The adapter omits Laya's duplicate
`answer_confidence` and unrelated `action.act_probability` fields.

Automated YASP tests should mock the decision API and must not install Laya or
download model weights. Stop the server with Ctrl+C.
