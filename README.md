rm -rf test-data/main/ && \
  RUST_LOG=info \
  AGRAPHSTORE_PATH='test-data/main' \
  AGRAPHSTORE_SERVER_URL='http://127.0.0.1:50000' \
  AGRAPHSTORE_INITIAL_REMOTE_URLS='' \
  MAIN=true \
  cargo run

rm -rf test-data/remote/ && \
  RUST_LOG=info \
  AGRAPHSTORE_PATH='test-data/remote' \
  AGRAPHSTORE_SERVER_URL='http://127.0.0.1:50001' \
  AGRAPHSTORE_INITIAL_REMOTE_URLS='http://127.0.0.1:50000' \
  cargo run
